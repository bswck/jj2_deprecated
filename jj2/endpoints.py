import asyncio
import functools
import inspect
import ipaddress
import multiprocessing
import socket
import threading
import typing
import warnings
import weakref

from jj2.constants import DEFAULT_COMMUNICATION_ENCODING

if typing.TYPE_CHECKING:
    from asyncio import StreamReader
    from asyncio import StreamWriter
    from typing import Any
    from typing import ClassVar
    from typing import Callable  # noqa: F401


_IPAddressT = typing.ForwardRef('ipaddress.IPv4Address | ipaddress.IPv6Address | None')


class Connection:
    """Object that stores all the connection-related data and implements asynchronous behaviour."""
    MSG_ENCODING: 'ClassVar[str]' = DEFAULT_COMMUNICATION_ENCODING
    IP_UNKNOWN: 'ClassVar[str]' = '0.0.0.0'
    IP_LOCALHOST: 'ClassVar[str]' = '127.0.0.1'

    def __init__(
        self, 
        template: 'EndpointTemplate',
        reader: 'StreamReader', 
        writer: 'StreamWriter'
    ):
        self.template = template
        self.reader = reader
        self.writer = writer

        self._host: '_IPAddressT' = ipaddress.ip_address(self.IP_UNKNOWN)
        self._domain: 'str | None' = None
        self._local_host: '_IPAddressT' = ipaddress.ip_address(self.IP_UNKNOWN)
        self._local_port: 'int | None' = None
        self._get_addr_and_fqdn()

        self._is_alive = True

    def _get_addr_and_fqdn(self):
        sock: 'socket.socket' = self.writer.get_extra_info('socket')

        rhost, rport = sock.getpeername()[:2]
        self._host = ipaddress.ip_address(rhost)
        self._port = rport
        self._domain = socket.getfqdn(self._host.compressed)

        lhost, lport = sock.getsockname()[:2]
        self._local_host = ipaddress.ip_address(lhost)
        self._local_port = rport
        self._local_domain = socket.getfqdn(self._local_host.compressed)

    @property
    def is_localhost(self) -> 'bool':
        return self._host.is_loopback

    @property
    def is_alive(self) -> 'bool':
        """Examine whether the connection is alive."""
        return self._is_alive

    @property
    def address(self) -> 'str':
        """The address of the remote endpoint in a 'domain:port' format."""
        return f'{self._domain}:{self._port}'

    @property
    def domain(self) -> 'str':
        """
        The domain of the remote endpoint.
        For instace if the remote endpoint host is '149.210.206.11', the domain is 'pukenukem.com'.
        """
        return self._domain

    @property
    def host(self) -> '_IPAddressT':
        """The IP address of the remote endpoint."""
        return self._host

    @property
    def port(self) -> 'int':
        """The port of the remote endpoint."""
        return self._port

    @property
    def local_address(self) -> 'str':
        """The address of the local endpoint in a 'local domain:local port' format."""
        return f'{self._local_domain}:{self._local_port}'

    @property
    def local_domain(self) -> 'str':
        """The domain of the local endpoint."""
        return self._local_domain

    @property
    def local_host(self) -> '_IPAddressT':
        """The IP address of the local endpoint."""
        return self._local_host

    @property
    def local_port(self) -> int:
        """The port of the local endpoint."""
        return self._local_port

    def kill(self):
        """Immediately stop the connection communication."""
        self._is_alive = False

    @functools.singledispatchmethod
    def write(self, data: 'str | bytes'):
        """Send data through the connection, bytes or string."""
        warnings.warn(f'unknown write() data type {type(data).__name__}, defaulting to message()')
        self.message(data)

    @write.register(bytes)
    def send(self, data: 'bytes'):
        self.writer.write(data)

    @write.register(str)
    def message(self, data: 'str'):
        self.send(data.encode(self.MSG_ENCODING))

    def read(self, n: 'int' = -1):
        self.reader.read(n)

    def sync(self, pool: 'ConnectionPool', data: 'str | bytes', exclude_self: 'bool' = True):
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

    def on_sync(self, pool: 'ConnectionPool', data: 'str | bytes'):
        """
        Receive synchronization request from :param:`pool`
        to send :param:`data` through this connection.
        """
        self.write(data)

    async def validate(self, pool: 'ConnectionPool | None' = None):
        """
        This method is called before the main connection loop starts.
        It is intended to call kill(), if the connection cannot be validated.

        Parameters
        ----------
        pool : ConnectionPool or None
            Connection pool instance that requested validation.
        """

    async def communicate(self, pool: 'ConnectionPool | None' = None):
        """
        A single cycle in the connection life.
        This method is intended to interact with the connection I/O through write() or receive().
        It's called continuously by an external loop, and it can be stopped by kill().

        Parameters
        ----------
        pool : ConnectionPool or None
            Connection pool instance that requested validation.
        """

    def __init_subclass__(cls):
        write_meth = typing.cast(functools.singledispatchmethod, cls.write)
        if cls.write is Connection.write:
            write_meth = functools.singledispatchmethod(write_meth.func)
        for func in write_meth.dispatcher.registry.values():
            write_meth.register(getattr(cls, func.__name__))


class ConnectionPool:
    """
    NOTE: Do not mistake with database<->application connection pools.
    Connection pool that stores all the connections and also may keep them alive.
    Use it to impose custom behavior on all of them across many various servers,
    e.g. for setting timeout on each, but also for storing connections
    of various endpoint templates (server connections, client connections etc.).
    """

    def __init__(self, future: 'asyncio.Future | None' = None):
        if future is None:
            loop = asyncio.get_event_loop()
            future = loop.create_future()
        self.future = future
        self.future.add_done_callback(self.on_future_done)
        self.tasks = []
        self.connections = weakref.WeakSet()

    def sync(self, data: 'str | bytes', *exclude_conns: 'Connection'):
        connections = self.connections[:]
        if exclude_conns:
            for excluded_conn in exclude_conns:
                connections.discard(excluded_conn)
        for connection in connections:
            connection.on_sync(self, data)

    def cancel(self):
        """Cancel pending and current connection-related tasks."""
        self.cancel_pending_tasks()
        self.cancel_current_tasks()

    def on_future_done(self):
        """Pool future done callback."""
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
            await connection.communicate(self)
            if not connection.is_alive:
                self.connections.remove(connection)
                return

    def task(self, task: 'typing.Coroutine'):
        loop = asyncio.get_running_loop()
        if isinstance(task, typing.Coroutine):
            task = loop.create_task(task)
        self.tasks.append(task)

    async def on_connection(
        self, 
        endpoint: 'EndpointTemplate', 
        reader: 'StreamReader', 
        writer: 'StreamWriter'
    ):
        connection = endpoint.create_connection(reader, writer)
        await connection.validate()
        await self.run(connection)

    def connection_callback(self, endpoint):
        return lambda reader, writer: self.task(
            self.on_connection(endpoint, reader, writer)
        )


class EndpointTemplate:
    """
    Endpoint template that describes architecture (client-server, P2P) and stores related
    connections. It works with ConnectionPool, which manages to keep those connections alive
    by calling proper connection's methods in asynchronous loop.

    To start an endpoint, use start() method.
    You can set default host and port as class attributes.
    """

    default_host: 'str | None' = None
    default_port: 'int | None' = None
    connection_class: 'type[Connection]' = Connection
    pool_class: 'type[ConnectionPool]' = ConnectionPool

    def __init__(
        self,
        pool: 'ConnectionPool | None' = None,
        connection_class: 'type[Connection] | None' = None,
        **endpoint_kwargs: 'Any'
    ):
        if pool is None:
            pool = self.pool_class()
        self.pool = pool

        self.connections: 'list[connection_class]' = []
        self.endpoint_kwargs = endpoint_kwargs

        self._endpoints: 'list' = []
        self._loop: 'asyncio.AbstractEventLoop | None' = None
        self._future: 'asyncio.Future | None' = None

        if connection_class:
            self.connection_class = connection_class

    @property
    def is_ssl(self) -> 'bool':
        return self.endpoint_kwargs.get('ssl_context') is not None

    def create_connection(
        self, 
        reader: 'StreamReader', 
        writer: 'StreamWriter'
    ) -> 'Connection':
        connection = self.connection_class(self, reader, writer)
        self.connections.append(connection)
        return connection

    def start(
        self,
        scheduler: 'Callable[[Endpoint], ...] | None' = None,
        blocking: 'bool' = True
    ):
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

    def start_concurrent(
        self,
        cls: 'type',
        scheduler: 'Callable[[Endpoint], ...] | None' = None,
        blocking: 'bool' = True,
        timeout: 'int | None' = None
    ):
        concurrent = cls(
            target=self.start,
            kwargs=dict(scheduler=scheduler, blocking=True)
        )
        concurrent.start()
        if blocking:
            concurrent.join(timeout)
        return concurrent

    def start_thread(
        self,
        scheduler: 'Callable[[Endpoint], ...] | None' = None,
        blocking: 'bool' = True,
        timeout: 'int | None' = None
    ):
        """Start an endpoint in a separate thread."""
        return self.start_concurrent(threading.Thread, scheduler, blocking, timeout)

    def start_process(
        self,
        scheduler: 'Callable[[Endpoint], ...] | None' = None,
        blocking: 'bool' = True,
        timeout: 'int | None' = None
    ):
        """Start an endpoint in a separate process."""
        return self.start_concurrent(multiprocessing.Process, scheduler, blocking, timeout)

    def stop(self):
        """Stop all connections bound to this endpoint template."""

        if self.connections:
            self._future.cancel()
            self.pool.cancel()
            self.connections.clear()

    def create_endpoint(
        self,
        host: 'str | None' = None,
        port: 'int | None' = None,
        **endpoint_kwargs: 'Any'
    ):
        """
        Create and return the asynchronous endpoint. Could be a server, client, peer etc.

        Parameters
        ----------
        host : str
            Host of the endpoint.
        port : int
            Port of the endpoint.
        endpoint_kwargs : Any
            Additional arguments to the endpoint constructor.
        """
        endpoint_kwargs = {**self.endpoint_kwargs, **endpoint_kwargs}
        if host is None:
            host = self.default_host
        if host is None:
            raise ValueError('host was not provided')
        if port is None:
            port = self.default_port
        if port is None:
            raise ValueError(f'{host}: port was not provided')

        return self.endpoint_factory(host, port, **endpoint_kwargs)

    def endpoint_factory(
        self,
        host: 'str',
        port: 'int',
        **endpoint_kwargs: 'Any'
    ) -> 'tuple[StreamReader, StreamWriter]':
        """Create and return the asynchronous endpoint. Could be a server, client etc."""
        raise NotImplementedError


class Server(EndpointTemplate):
    _endpoints: 'list[asyncio.AbstractServer]'

    def endpoint_factory(self, host, port, **endpoint_kwargs):
        endpoint_kwargs.update(host=host, port=port)
        return asyncio.start_server(self.pool.connection_callback(self), **endpoint_kwargs)


class Client(EndpointTemplate):
    _endpoints: 'list[tuple[StreamReader, StreamWriter]]'

    def endpoint_factory(self, host, port, **endpoint_kwargs):
        endpoint_kwargs.update(host=host, port=port)
        reader, writer = asyncio.open_connection(**endpoint_kwargs)
        self.create_connection(reader, writer)
        return reader, writer
