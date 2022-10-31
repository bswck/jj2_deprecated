import asyncio
import functools
import inspect
import typing
import warnings
import weakref

from jj2.constants import DEFAULT_COMMUNICATION_ENCODING


class Connection:
    """Object that stores all the connection-related data and implements asynchronous behaviour."""
    MSG_ENCODING = DEFAULT_COMMUNICATION_ENCODING
    IP_UNKNOWN = '0.0.0.0'

    def __init__(self, endpoint, reader, writer):
        sockname = (
            writer.get_extra_info('peername')
            or writer.get_extra_info('sockname')
        )
        self.address = None
        if sockname:
            self.address = sockname[0]
        self.endpoint = endpoint
        self.reader = reader
        self.writer = writer

        self._is_alive = True
        self._lock = asyncio.Lock()

    @property
    def is_localhost(self):
        return self.address.lower() in ("127.0.0.1", "localhost", self.endpoint.host)

    @property
    def is_alive(self):
        return self._is_alive

    def kill(self):
        self._is_alive = False

    @functools.singledispatchmethod
    def write(self, data):
        warnings.warn(f'unknown write data type {type(data).__name__}, defaulting to message()')
        self.message(data)

    @write.register
    def send(self, data: bytes):
        self.writer.write(data)

    @write.register
    def message(self, data: str):
        self.send(data.encode(self.MSG_ENCODING))

    def receive(self, n: int = -1):
        self.reader.read(n)

    async def cycle(self, pool=None):
        """
        A coroutine-safe single cycle in the connection life.
        This method is intended to interact with the connection I/O through write() or receive().
        It's called continuously by an external loop, and it can be stopped by kill().

        Parameters
        ----------
        pool : ConnectionPool or None
            Connection pool instance that requested validation.
        """
        async with self._lock:
            return await self._cycle(pool)

    async def _validate(self, pool: 'ConnectionPool | None' = None):
        """
        This method is called before the main connection loop starts.
        It is intended to call kill(), if the connection cannot be validated.

        Parameters
        ----------
        pool : ConnectionPool or None
            Connection pool instance that requested validation.
        """

    async def _cycle(self, pool: 'ConnectionPool | None' = None):
        """
        A single cycle in the connection life.
        This method is intended to interact with the connection I/O through write() or receive().
        It's called continuously by an external loop, and it can be stopped by kill().

        Parameters
        ----------
        pool : ConnectionPool or None
            Connection pool instance that requested validation.
        """

    def sync(self, pool: 'ConnectionPool', data: str | bytes, exclude_self=True):
        """
        Synchronize :param:`data` in the entire :param:`pool`.
        Exclude this connection by default.

        Parameters
        ----------
        pool : ConnectionPool
            Connection pool instance of connections to synchronize the data through.
        data :

        exclude_self
        """
        if exclude_self:
            pool.sync(data, self)
        else:
            pool.sync(data)

    def __init_subclass__(cls):
        write_meth = typing.cast(functools.singledispatchmethod, cls.write)
        if cls.write is Connection.write:
            write_meth = functools.singledispatchmethod(write_meth.func)
        for func in write_meth.dispatcher.registry.values():
            write_meth.register(getattr(cls, func.__name__))


class ConnectionPool:
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

    def sync(self, data, *exclude_conns):
        connections = self.connections[:]
        if exclude_conns:
            for excluded_conn in exclude_conns:
                connections.discard(excluded_conn)
        for connection in connections:
            connection.write(data)

    def cancel(self):
        self.cancel_pending_tasks()
        self.cancel_current_tasks()

    def on_closed(self):
        self.cancel_current_tasks()

    def cancel_pending_tasks(self):
        self.future.cancel()

    def cancel_current_tasks(self):
        for task in self.tasks:
            task.cancel()

    async def run(self, connection):
        if not connection.is_alive:
            return

        self.connections.add(connection)

        while not (self.future.done() or self.future.cancelled()):
            await connection.cycle(self)
            if not connection.is_alive:
                self.connections.remove(connection)
                return

    def task(self, task):
        loop = asyncio.get_running_loop()
        if isinstance(task, typing.Coroutine):
            task = loop.create_task(task)
        self.tasks.append(task)

    async def on_connection(self, endpoint, reader, writer):
        connection = endpoint.connection(reader, writer)
        connection.is_alive = await connection._validate()
        await self.run(connection)

    def connection_callback(self, endpoint):
        return lambda reader, writer: self.task(
            self.on_connection(endpoint, reader, writer)
        )


class Endpoint:
    default_host = None
    default_port = None
    connection_class = Connection
    pool_class = ConnectionPool
    sync_pool_class = ConnectionPool

    def __init__(
        self,
        pool=None,
        connection_class=None,
        **endpoint_args
    ):
        if pool is None:
            pool = self.pool_class()
        self.pool = pool

        self.connection_class = (
            connection_class or self.connection_class
        )
        self.connections = []
        self.endpoint_args = endpoint_args

        self._loop = None
        self._future = None

    @property
    def is_ssl(self):
        return self.endpoint_args.get('ssl_context') is not None

    def connection(self, reader, writer):
        connection = self.connection_class(self, reader, writer)
        self.connections.append(connection)
        return connection

    def start(self, scheduler=None, blocking=True):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._future = loop.create_future()

        if callable(scheduler):
            mby_coro = scheduler(self)
            if inspect.isawaitable(mby_coro):
                loop.create_task(mby_coro)

        if blocking:
            loop.run_until_complete(self._future)

    def stop(self):
        if self.connections:
            self._future.cancel()
            self.pool.cancel()
            self.connections.clear()

    def endpoint(self, host=None, port=None, **endpoint_args):
        if host is None:
            host = self.default_host
        if host is None:
            raise ValueError('host was not provided')
        if port is None:
            port = self.default_port
        if port is None:
            raise ValueError(f'{host}: port was not provided')
        endpoint_args = {**self.endpoint_args, **endpoint_args}
        return self.create_endpoint(host, port, **endpoint_args)

    def create_endpoint(self, host, port, **endpoint_args):
        raise NotImplementedError


class Server(Endpoint):
    def create_endpoint(self, host, port, **endpoint_args):
        endpoint_args.update(host=host, port=port)
        return asyncio.start_server(self.pool.connection_callback(self), **endpoint_args)


class Client(Endpoint):
    def create_endpoint(self, host, port, **endpoint_args):
        endpoint_args.update(host=host, port=port)
        return asyncio.open_connection(**endpoint_args)
