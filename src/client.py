from src.base import AsyncBase
from uasyncio import create_task as launch, sleep_ms as sleep, gather
from utime import ticks_ms as ticks, ticks_diff as diff
import gc
import socket


class MQTTClient(AsyncBase):

    def __init__(self, server, port, uid = None, user = None, password = None):
        super(MQTTClient, self).__init__(server, port, uid, user, password)
        self.__isconnected_broker = False  # sí está conectado {broker}
        self.__inconnect_broker = False  # connexión en curso {broker}
        self.__isconnected_wlan = False  # se ha conectado {WiFi}
        # handler
        self.__subscritions: list = list()

    def tosubscribe(self, topic: bytes, qos: int):
        assert qos in (0, 1)
        self.__subscritions.append(topic)

    async def disconnect(self):
        """
        Desconecta el cliente del intermediario
        :return:
        """
        try:
            async with self.__lock:
                self.__sock.write(b"\xe0\0")
        except OSError:
            pass
        if self.__sock is not None:
            self.__sock.close()
            self.__sock = None
        self.__isconnected_broker = False
        self.__wlan.disconnect()
        self.__wlan.active(False)
        if bool(self.__green) and bool(self.__red) and bool(self.__blue):
            self.__red.off()
            self.__green.off()
            self.__blue.off()

    def isconnected(self) -> bool:
        if self.__inconnect_broker:  # Disable low-level check during .connect()
            return True
        if self.__isconnected_broker and not self.__wlan.isconnected():  # It's going down.
            await self.__reconnect()
        return self.__isconnected_broker

    async def connect(self):
        if not self.__wlan.isconnected():
            await self.connectwifi()
        self.__inconnect_broker = True  # Disable low level ._isconnected check
        while True:
            self.log('[──] «MQTT» :connecting...')
            await self.__blink(self.__blue, 6, 500)
            if self.__wlan.isconnected():
                try:
                    await self.__connect()
                    await self.__blink(self.__green, 6, 500)
                    self.log('[OK] «MQTT» :successful connection!!!')
                except OSError:
                    self.__close()
                else:
                    await self.__blink(self.__green)
                    break
            else:
                await self.connectwifi()

        self.__pids.clear()
        self.__isconnected_broker = True
        self.__inconnect_broker = False  # Low level code can now check connectivity.
        await gather(
            self.__message(),
            self.__keep_alive(),
            self.__memory(),
            self.__connected()
        )
        # launch(self.__message())
        # launch(self.__keep_alive())
        # if self.__debug:
        #     launch(self.__memory())
        #
        # launch(self.__connected())

    # Launched by .connect(). Runs until connectivity fails. Checks for and
    # handles incoming messages.
    async def __message(self):
        try:
            while self.isconnected():
                async with self.__lock:
                    await self.wait_msg()  # Immediate return if no message
                await sleep(1)  # Permitir que otras tareas se bloqueen
        except OSError:
            pass
        await self.__reconnect()  # Broker or WiFi fail.

    # DEBUG: show RAM messages.
    async def __memory(self):
        count = 0
        while self.isconnected():  # Ensure just one instance.
            await sleep(1000)  # Quick response to outage.
            count += 1
            count %= 20
            if not count:
                gc.collect()
                print('[──] «MQTT» :ram free', gc.mem_free(), 'alloc', gc.mem_alloc())

    async def __keep_alive(self):
        while self.isconnected():
            pings_due = diff(ticks(), self.__last_rx) // self.__ping_interval
            if pings_due >= 4:
                self.log('Reconnect: broker fail.')
                break
            await sleep(self.__ping_interval)
            try:
                await self.__ping()
            except OSError:
                break
        await self.__reconnect()  # Broker or WiFi fail.

    async def __connected(self):
        print("conn")
        for topic in self.__subscritions:
            print(topic)
            await self.subscribe(topic, 1)

    async def __reconnect(self):  # Programe una reconexión si no está en marcha.
        if self.__isconnected_broker:
            self.__isconnected_broker = False
            self.__close()
            await sleep(2000)
            await self.connect()

    async def wan_ok(
        self,
        packet = b'$\x1a\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x03www\x06google\x03com\x00\x00\x01\x00\x01'
    ):
        if not self.isconnected():  # Wi-Fi is down
            return False
        length = 32  # DNS query and response packet size
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setblocking(False)
        s.connect(('8.8.8.8', 53))
        await sleep(1000)
        try:
            await self.__write(packet, sock = s)
            await sleep(2000)
            res = await self.__read(length, s)
            if len(res) == length:
                return True  # DNS response size OK
        except OSError:  # Timeout on read: no connectivity.
            return False
        finally:
            s.close()
        return False

    async def broker_up(self):
        """
        Prueba de conectividad del intermediario
        :return:
        """
        if not self.isconnected():
            return False
        tlast = self.__last_rx
        if diff(ticks(), tlast) < 1000:
            return True
        try:
            await self.__ping()
        except OSError:
            return False
        t = ticks()
        while not self.__timeout(t):
            await sleep(100)
            if diff(self.__last_rx, tlast) > 0:  # Respuesta recibida
                return True
        return False

    # Await broker connection.
    async def __wait_connection(self):
        while not self.__isconnected_broker:
            await sleep(1000)

    async def subscribe(self, topic, qos = 0):
        assert qos in (0, 1)
        while True:
            await self.__wait_connection()  # espera hasta que hay una conexión.
            try:
                return await super().subscribe(topic, qos)
            except OSError:
                await self.__reconnect()  # Broker or WiFi fail.

    async def publish(self, topic, msg, qos = 0, retain = False):
        assert qos in (0, 1)
        while True:
            await self.__wait_connection()  # espera hasta que haya la conexión.
            try:
                return await super().publish(topic, msg, retain, qos)
            except OSError:
                await self.__reconnect()  # Broker or WiFi fail.
