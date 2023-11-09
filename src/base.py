import gc

import usocket as socket
import ustruct as struct
from utime import ticks_ms as ticks, ticks_diff as diff
from uerrno import EINPROGRESS, ETIMEDOUT
from micropython import const
from uasyncio import sleep_ms as sleep, Lock, gather, get_event_loop as running, create_task as launchtask
from lib.tools import pidgenerator
from machine import Pin
from network import WLAN, STA_IF

gc.collect()


class MQTTException(Exception):
    pass


class AsyncBase:
    DEFAULT = const(2)
    SOCKET_POLL_DELAY = const(5)
    BUSY_ERRORS = [EINPROGRESS, ETIMEDOUT, 118, 119]
    REPUB_COUNT = 0  # TEST

    def __init__(self, server, port, uid = None, user = None, password = None):
        self.__server = server
        self.__port = port
        self.__uid = uid
        self.__user = user
        self.__password = password
        # Wi-Fi
        self.__ssid = None
        self.__pwd = None
        # MQTT config
        self.__keepalive = 60
        if self.__keepalive >= 65536:
            raise ValueError('invalid keepalive time')
        self.__response_time = 10 * 1000  # Repub if no PUBACK received (ms).
        self.__max_repubs = 4
        self.__clean = True  # clean_session state on reconnect
        # ssl
        self.__ssl = dict()
        # broker
        self.__sock = None

        self.__newpid = pidgenerator()
        self.__pids = set()  # PUBACK and SUBACK pids awaiting ACK response
        self.__last_rx = ticks()  # Time of last communication from broker
        self.__lock = Lock()

        # last will and testament
        self.__lw_topic = None
        self.__lw_msg = None
        self.__lw_qos = None
        self.__lw_retain = None

        self.__ping_interval = 0

        keepalive = 1000 * self.__keepalive  # keepalive = 60000
        self.__ping_interval = keepalive // 4 if keepalive else 20000  # ping_interval = 20000
        pi = self.__ping_interval * 1000  # pi = 20000000
        if pi and pi < self.__ping_interval:
            self.__ping_interval = pi

        self.__response_time = 10
        self.__debug = False

        self.__red = None
        self.__green = None
        self.__blue = None

        # WiFi
        self.__wlan = WLAN(STA_IF)

        self.__callback = None

    def setcallback(self, function):
        self.__callback = function

    async def __aiter__(self):
        pass

    async def __blink(self, led: Pin, repetitions: int = 1, delay: int = 500):
        if bool(self.__green) and bool(self.__red) and bool(self.__blue):
            self.__green.off()
            self.__red.off()
            self.__blue.off()
            await sleep(delay)
            if repetitions > 1:
                for _ in range(repetitions):
                    led.value(not led.value())
                    await sleep(delay)
            led.on()
        await sleep(2000)

    async def connectwifi(self) -> None:
        assert self.__ssid and self.__pwd, 'Wi-Fi requiere ssid y password'
        self.__wlan.active(True)
        self.__wlan.connect(self.__ssid, self.__pwd)
        while True:
            self.log('[--] «WiFi» :starting connection...')
            await self.__blink(self.__red, 6, 500)
            if self.__wlan.isconnected():
                self.log('[OK] «WiFi» :successful connection', self.__wlan.ifconfig()[0])
                break

    def __timeout(self, t) -> int:
        return diff(ticks(), t) > self.__response_time

    def setlwt(self, topic: bytes, msg: bytes, qos: int = 0, retain: bool = False) -> None:
        assert qos in (0, 1) and topic
        self.__lw_topic = topic
        self.__lw_msg = msg
        self.__lw_qos = qos
        self.__lw_retain = retain

    def confing(self, ssid, pwd, red, green, blue, debug):
        self.__ssid = ssid
        self.__pwd = pwd
        self.__debug = debug
        if bool(red) and bool(green) and bool(blue):
            self.__red = Pin(red, Pin.OUT, value = 0)
            self.__green = Pin(green, Pin.OUT, value = 0)
            self.__blue = Pin(blue, Pin.OUT, value = 0)

    def log(self, *args) -> None:
        if self.__debug:
            print(*args)

    async def __read(self, size: int, sock = None) -> bytes:  # OSError caught by superclass
        if sock is None:
            sock = self.__sock
        data = b''
        t = ticks()
        while len(data) < size:
            if self.__timeout(t):
                raise OSError(-1)
            try:
                msg = sock.read(size - len(data))
            except OSError as e:  # ESP32 issues weird 119 errors here
                msg = None
                if e.args[0] not in self.BUSY_ERRORS:
                    raise
            if msg == b'':  # Connection closed by host
                raise OSError(-1)
            if msg is not None:  # data received:
                # tuple = (b'Python', b'is') -> print(b' '.join(lst)) -> b'Python is beaut'
                data = b''.join((data, msg))
                t = ticks()
                self.__last_rx = ticks()
            await sleep(self.SOCKET_POLL_DELAY)
        return data

    async def __write(self, value: bytes, length = 0, sock = None) -> None:
        if sock is None:
            sock = self.__sock
        if length:
            value = value[:length]
        t = ticks()
        while value:
            if self.__timeout(t) or not self.isconnected():
                raise OSError(-1)
            try:
                n = sock.write(value)
            except OSError as e:  # ESP32 issues weird 119 errors here
                n = 0
                if e.args[0] not in self.BUSY_ERRORS:
                    self.log('[><] «MQTT» :connection to broker failed')
                    raise
            if n:
                t = ticks()
                value = value[n:]
            await sleep(self.SOCKET_POLL_DELAY)

    async def __send(self, value: bytes) -> None:
        await self.__write(struct.pack("!H", len(value)))  # size
        await self.__write(value)  # msg

    async def __recv(self) -> int:
        n = 0
        sh = 0
        while True:
            res = await self.__read(size = 1)
            b = res[0]
            n |= (b & 0x7f) << sh
            if not b & 0x80:
                return n
            sh += 7

    async def __encode(self, size, premsg) -> int:
        assert size < 268435456  # 2**28, i.e. max. four 7-bit bytes
        i = 1
        while size > 0x7f:
            premsg[i] = (size & 0x7f) | 0x80
            size >>= 7
            i += 1
        premsg[i] = size
        return i

    async def __connect(self) -> None:
        self.__sock = socket.socket()
        self.__sock.setblocking(False)
        try:
            addr = socket.getaddrinfo(self.__server, self.__port)[0][-1]
            self.__sock.connect(addr)
        except OSError as e:
            if e.args[0] not in self.BUSY_ERRORS:
                self.log('[><] «MQTT» :no network connection')
                raise
        await sleep(self.DEFAULT)
        if bool(self.__ssl):
            import ussl
            self.__sock = ussl.wrap_socket(self.__sock, **self.__ssl)
        premsg = bytearray(b"\x10\0\0\0\0\0")
        msg = bytearray(b"\x04MQTT\x04\0\0\0")  # Protocol 3.1.1

        size = 10 + 2 + len(self.__uid)
        msg[6] = self.__clean << 1
        if self.__user:
            size += 2 + len(self.__user) + 2 + len(self.__password)
            msg[6] |= 0xC0
        if self.__keepalive:
            msg[7] |= self.__keepalive >> 8
            msg[8] |= self.__keepalive & 0x00FF
        if self.__lw_topic:
            size += 2 + len(self.__lw_topic) + 2 + len(self.__lw_msg)
            msg[6] |= 0x4 | (self.__lw_qos & 0x1) << 3 | (self.__lw_qos & 0x2) << 3
            msg[6] |= self.__lw_retain << 5

        i = await self.__encode(size, premsg)

        await self.__write(value = premsg, length = i + 2)
        await self.__write(value = msg)
        await self.__send(self.__uid)
        if self.__lw_topic:
            await self.__send(self.__lw_topic)
            await self.__send(self.__lw_msg)
        if self.__user:
            await self.__send(self.__user)
            await self.__send(self.__password)
        # Await CONNACK
        # read causes ECONNABORTED if brokecr is out; triggers a reconnect.
        resp = await self.__read(4)
        if resp[3] != 0 or resp[0] != 0x20 or resp[1] != 0x02:
            self.log('[><] «MQTT» :authentication fail')
            raise OSError(-1)  # Bad CONNACK e.g. authentication fail.

    async def __ping(self):
        async with self.__lock:
            await self.__write(b"\xc0\0")

    def __close(self):
        if self.__sock is not None:
            self.__sock.close()
            self.__sock = None

    async def __await_pid(self, pid) -> bool:
        time = ticks()
        while pid in self.__pids:  # copia local
            if self.__timeout(time):
                break  # Debe repub o rescatar
            await sleep(100)
        else:
            return True  # PID recibido Todo listo
        return False

    # qos == 1: coro bloquea hasta que wait_msg obtiene el PID correcto.
    # Si WiFi falla por completo, la subclase vuelve a publicar con un nuevo PID.
    async def publish(self, topic, msg, retain, qos) -> None:
        pid = next(self.__newpid)
        if qos:
            self.__pids.add(pid)
        async with self.__lock:
            await self.__publish(topic, msg, retain, qos, 0, pid)
        if qos == 0:
            return

        count = 0
        while True:  # Espere PUBACK, vuelva a publicar en el tiempo de espera
            if await self.__await_pid(pid):
                return
            # No match
            if count >= self.__max_repubs:
                raise OSError(-1)  # Subclase para volver a publicar con nuevo PID

            async with self.__lock:
                await self.__publish(topic, msg, retain, qos, dup = 1, pid = pid)  # Añadir pid
            count += 1
            self.REPUB_COUNT += 1

    async def __publish(self, topic, msg, retain, qos, dup, pid):
        pkt = bytearray(b"\x30\0\0\0")
        pkt[0] |= qos << 1 | retain | dup << 3
        size = 2 + len(topic) + len(msg)
        if qos > 0:
            size += 2
        if size >= 2097152:
            raise MQTTException('Strings too long.')

        i = await self.__encode(size, pkt)

        await self.__write(pkt, i + 1)
        await self.__send(topic)
        if qos > 0:
            struct.pack_into("!H", pkt, 0, pid)
            await self.__write(pkt, 2)
        await self.__write(msg)

    # Puede generar OSError si WiFi falla. Trampas de subclase
    async def subscribe(self, topic, qos):
        pkt = bytearray(b"\x82\0\0\0")
        pid = next(self.__newpid)
        self.__pids.add(pid)
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, pid)
        async with self.__lock:
            await self.__write(pkt)
            await self.__send(topic)
            await self.__write(qos.to_bytes(1, "little"))

        if not await self.__await_pid(pid):
            raise OSError(-1)

    # Espere un solo mensaje MQTT entrante y procéselo.
    # Los mensajes suscritos se entregan a una devolución de llamada previamente
    # establecida por el método .setup(). Otro MQTT (interno)
    # mensajes procesados ​​internamente.
    # Devolución inmediata si no hay datos disponibles. Llamado desde ._handle_msg().
    async def wait_msg(self):
        try:
            resp = self.__sock.read(1)  # Lanza OSError en falla de Wi-Fi
        except OSError as e:
            if e.args[0] in self.BUSY_ERRORS:  # Necesario por RP2
                await sleep(0)
                return
            raise

        if resp is None:
            return
        print('respuesta:', resp, 'op:', resp[0])
        if resp == b'':  # connection fail
            self.log('[><] «MQTT» :connection fail to broker')
            raise OSError(-1)

        if resp == b"\xd0":  # PINGRESP
            await self.__read(1)  # Update .last_rx time
            return
        op = resp[0]
        print('op:', op)
        if op == 0x40:  # PUBACK: save pid
            size = await self.__read(1)
            if size != b"\x02":
                raise OSError(-1)
            rcv_pid = await self.__read(2)
            pid = rcv_pid[0] << 8 | rcv_pid[1]
            if pid in self.__pids:
                self.__pids.discard(pid)
            else:
                raise OSError(-1)

        if op == 0x90:  # SUBACK
            resp = await self.__read(4)
            if resp[3] == 0x80:
                raise OSError(-1)
            pid = resp[2] | (resp[1] << 8)
            if pid in self.__pids:
                self.__pids.discard(pid)
            else:
                raise OSError(-1)

        if op & 0xf0 != 0x30:
            return
        size = await self.__recv()
        topic_len = await self.__read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = await self.__read(topic_len)
        size -= topic_len + 2
        if op & 6:
            pid = await self.__read(2)
            pid = pid[0] << 8 | pid[1]
            size -= 2
        msg = await self.__read(size)
        qos = op & 6
        retained = op & 0x01
        # await gather(self.__callback(topic, msg, qos, bool(retained)))
        launchtask(self.__callback(topic, msg, qos, bool(retained)))
        self.log(
            '[──] «MQTT»',
            ':topic ->', topic.decode('UTF-8'),
            ', msg ->', msg.decode('UTF-8'),
            ', qos ->', qos,
            ', retained ->', bool(retained)
        )
        if qos == 2:  # qos 1: confirma que a recivido el mesange
            pkt = bytearray(b"\x40\x02\0\0")  # Send PUBACK
            struct.pack_into("!H", pkt, 2, pid)
            await self.__write(pkt)
        elif qos == 4:  # qos 2 not supported
            raise OSError(-1)
