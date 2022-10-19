import asyncio
import typing
import weakref


class Connection:
    msg_encoding = 'CP1250'

    def __init__(self, server, reader, writer):
        sockname = (
            writer.get_extra_info('peername')
            or writer.get_extra_info('sockname')
        )
        self.ip = None
        if sockname:
            self.ip = sockname[0]
        self.server = server
        self.reader = reader
        self.writer = writer
        self.is_alive = True

        self.address = (
            f'{self.ip or "unknown"}:{self.server.port}'
        )

    def send(self, data: bytes):
        self.writer.write(data)

    def msg(self, string: str):
        self.send(string.encode(self.msg_encoding))

    def recv(self, n: int = -1):
        self.reader.read(n)

    def kill(self):
        self.is_alive = False

    @property
    def is_localhost(self):
        return self.ip.lower() in ("127.0.0.1", "localhost", self.server.host)

    async def validate(self):
        pass

    async def serve(self):
        pass


class ConnectionPool:
    """
    Connection pool that holds all the connections alive.
    Use it to impose custom behavior on all of them across many various servers,
    e.g. for setting timeout on each.
    """

    def __init__(self, future=None):
        if future is None:
            loop = asyncio.get_event_loop()
            future = loop.create_future()
        self.future = future
        self.future.add_done_callback(self.on_closed)
        self.tasks = []
        self.connections = weakref.WeakSet()

    def cancel_pending(self):
        self.future.cancel()

    def cancel_current(self):
        for task in self.tasks:
            task.cancel()

    def cancel(self):
        self.cancel_pending()
        self.cancel_current()

    def on_closed(self):
        self.cancel_current()

    async def serve(self, connection):
        if not connection.is_alive:
            return

        self.connections.add(connection)

        while not (self.future.done() or self.future.cancelled()):
            await connection.serve()
            if not connection.is_alive:
                self.connections.remove(connection)
                return

    async def on_connection(self, server, reader, writer):
        connection = server.connection(reader, writer)
        connection.is_alive = await connection.validate()
        await self.serve(connection)

    def task(self, task):
        loop = asyncio.get_running_loop()
        if isinstance(task, typing.Coroutine):
            task = loop.create_task(task)
        self.tasks.append(task)

    def connection_callback(self, server):
        return lambda reader, writer: self.task(
            self.on_connection(server, reader, writer)
        )


class Server:
    default_port = None
    connection_class = Connection

    def __init__(
        self,
        host,
        port=None,
        pool=None,
        connection_class=None,
        **kwds
    ):
        self.host = host
        self.port = port or self.default_port

        if pool is None:
            pool = ConnectionPool()

        self.pool = pool
        self.connection_class = connection_class or self.connection_class
        self.connections = []
        self._server = None
        self.is_ssl = kwds.get('ssl_context') is not None
        self.kwds = kwds

    def connection(self, reader, writer):
        connection = self.connection_class(self, reader, writer)
        self.connections.append(connection)
        return connection

    def start(self):
        self._server = asyncio.start_server(
            self.pool.connection_callback(self),
            host=self.host,
            port=self.port,
            **self.kwds
        )

    def stop(self):
        if self._server:
            self.pool.cancel()
