import asyncio
import functools
import typing
import weakref


class AsyncConnection:
    """
    Object that stores all the connection-related data and implements behaviour.
    Store
    """
    MSG_ENCODING = 'CP1250'
    IP_DEFAULT = 'unknown'

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

        self.address = ':'.join((
            self.ip or self.IP_DEFAULT,
            str(self.server.port)
        ))

    def __init_subclass__(cls):
        syncer = functools.singledispatchmethod(cls.syncer)
        syncer.register(cls.send)
        syncer.register(cls.msg)

    @property
    def is_localhost(self):
        return self.ip.lower() in ("127.0.0.1", "localhost", self.server.host)

    def sync(self, msg, exclude_self=True):
        exclude_conns = ()
        if exclude_self:
            exclude_conns = self,
        self.server.sync_pool.sync(msg, *exclude_conns)

    def syncer(self, msg):
        raise TypeError(f'unknown sync data type {type(msg).__name__!r}')

    def send(self, data: bytes):
        self.writer.write(data)

    def msg(self, string: str):
        self.send(string.encode(self.MSG_ENCODING))

    def recv(self, n: int = -1):
        self.reader.read(n)

    def kill(self):
        self.is_alive = False

    async def validate(self):
        pass

    async def run(self):
        pass


class AsyncConnectionPool:
    """
    Connection pool that stores all the connections and also may keep them alive.
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

    def sync(self, msg, *exclude_conns):
        connections = self.connections[:]
        if exclude_conns:
            for excluded_conn in exclude_conns:
                connections.discard(excluded_conn)
        for connection in connections:
            connection.syncer(msg)

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
            await connection.run()
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


class AsyncEndpoint:
    default_port = None
    connection_class = AsyncConnection
    pool_class = AsyncConnectionPool
    sync_pool_class = pool_class

    def __init__(
        self,
        host,
        port=None,
        pool=None,
        sync_pool=None,
        connection_class=None,
        **endpoint_args
    ):
        self.host = host
        self.port = port or self.default_port

        if pool is None:
            pool = self.pool_class()
        self.pool = pool

        if sync_pool is None:
            sync_pool = self.sync_pool_class()
        self.sync_pool = sync_pool

        self.connection_class = connection_class or self.connection_class
        self.connections = []
        self.endpoint_args = endpoint_args

        self._endpoint = None
        self._loop = None
        self._future = None

    @property
    def is_ssl(self):
        return self.endpoint_args.get('ssl_context') is not None

    def connection(self, reader, writer):
        connection = self.connection_class(self, reader, writer)
        self.connections.append(connection)
        return connection

    def start(self, blocking=True):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._endpoint = self.create_endpoint()

        self._future = loop.create_future()

        if blocking:
            loop.run_until_complete(self._future)

    def stop(self):
        if self._endpoint:
            self._future.cancel()
            self.pool.cancel()
            self._endpoint = None

    def create_endpoint(self):
        raise NotImplementedError


class AsyncServer(AsyncEndpoint):
    def create_endpoint(self):
        return asyncio.start_server(
            self.pool.connection_callback(self),
            host=self.host,
            port=self.port,
            **self.endpoint_args
        )


class AsyncClient(AsyncEndpoint):
    def create_endpoint(self):
        return asyncio.open_connection(
            host=self.host,
            port=self.port,
            **self.endpoint_args
        )
