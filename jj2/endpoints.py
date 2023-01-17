from __future__ import annotations

import asyncio
import functools
import inspect
import ipaddress
import multiprocessing
import os
import socket
import sys
import threading
import traceback
import typing
import warnings
import weakref

from jj2.constants import CHAT_ENCODING

if typing.TYPE_CHECKING:
    from typing import Any
    from typing import ClassVar
    from typing import Callable


_IPAddressT = typing.ForwardRef('ipaddress.IPv4Address | ipaddress.IPv6Address | None')
_TimeoutT = typing.Union[int, float, None]
_AddressT = typing.Tuple[str, int]


class BaseEndpointHandler:
    MSG_ENCODING: ClassVar[str] = CHAT_ENCODING
    IP_UNKNOWN: ClassVar[str] = '0.0.0.0'
    COMMUNICATION_BACKEND_FLAG: ClassVar[str] = '__communicates_as__'
    VALIDATION_BACKEND_FLAG: ClassVar[str] = '__validates_as__'

    _communication_backends: ClassVar[functools.singledispatch]
    _validation_backends: ClassVar[functools.singledispatch]

    _host: _IPAddressT
    _local_host: _IPAddressT

    _domain: str | None = None
    _port: int | None = None
    _local_domain: str | None = None
    _local_port: int | None = None

    def __init__(
        self,
        future: asyncio.Future,
        endpoint: Endpoint,
    ):
        self.future = future
        self.endpoint = endpoint
        self.queue = asyncio.Queue()

        self._host = ipaddress.ip_address(self.IP_UNKNOWN)
        self._local_host = ipaddress.ip_address(self.IP_UNKNOWN)

    def __post_init__(self, **kwargs):
        pass

    @property
    def is_localhost(self) -> bool:
        return self._host.is_loopback

    @property
    def is_alive(self) -> bool:
        """Examine whether the connection is alive."""
        return not (self.future.done() or self.future.cancelled())

    @property
    def address(self) -> _AddressT:
        """The address of the remote endpoint in a (domain, port) format."""
        return self.domain, self.port

    @property
    def address_string(self) -> str:
        """The address of the remote endpoint in a string 'domain:port' format."""
        return f'{self.domain}:{self.port}'

    @property
    def domain(self) -> str:
        """
        The domain of the remote endpoint.
        For instace if the remote endpoint host is '149.210.206.11', the domain is 'pukenukem.com'.
        """
        return self._domain or self.IP_UNKNOWN

    @property
    def host(self) -> _IPAddressT:
        """The IP address of the remote endpoint."""
        return self._host

    @property
    def port(self) -> int:
        """The port of the remote endpoint."""
        return self._port or 0

    @property
    def local_address(self) -> str:
        """The address of the local endpoint in a 'local domain:local port' format."""
        return f'{self.local_domain}:{self.local_port}'

    @property
    def local_domain(self) -> str:
        """The domain of the local endpoint."""
        return self._local_domain or self.IP_UNKNOWN

    @property
    def local_host(self) -> _IPAddressT:
        """The IP address of the local endpoint."""
        return self._local_host

    @property
    def local_port(self) -> int:
        """The port of the local endpoint."""
        return self._local_port or 0

    def stop(self):
        """Immediately stop the connection communication."""
        if self.is_alive:
            self._stop()
            self.future.set_result(None)

    def _stop(self):
        """Perform all operations needed to stop."""

    async def sync(
            self,
            pool: HandlerPool,
            data: str | bytes,
            *args: Any,
            exclude_self: bool = True
    ):
        """
        Synchronize :param:`data` in the entire :param:`pool`.
        Exclude this connection by default.
        """
        if exclude_self:
            await pool.sync(data, *args, exclude_handlers=[self])
        else:
            await pool.sync(data, *args)

    # noinspection PyUnusedLocal
    async def on_sync(self, pool: HandlerPool, data: str | bytes):
        """
        Receive synchronization request from :param:`pool`
        to send :param:`data` through this connection.
        """
        raise NotImplementedError

    async def validate(self, pool: HandlerPool | None = None):
        """
        This method is called before the main connection loop starts.
        It is intended to call kill(), if the connection cannot be validated.

        Parameters
        ----------
        pool : HandlerPool or None
            Connection pool instance that requested validation.
        """
        callback = self._validation_backends(self.endpoint)
        if callback is NotImplemented:
            validate = self.validate_default
        else:
            validate = functools.partial(callback, self)
        return await validate(pool)

    async def validate_default(self, pool: HandlerPool | None = None):
        """
        This method is called before the main connection loop starts.
        It is intended to call kill(), if the connection cannot be validated.

        Parameters
        ----------
        pool : HandlerPool or None
            Connection pool instance that requested validation.
        """
        pass

    async def communicate(self, pool: HandlerPool | None = None):
        """
        A single frame in the connection life.
        This method is intended to interact with the connection I/O through write() or/and read().
        It's called continuously by an external loop, and it can be stopped by kill().

        Parameters
        ----------
        pool : HandlerPool or None
            Connection pool instance that requested validation.
        """
        callback = self._communication_backends(self.endpoint)
        if callback is NotImplemented:
            communicate = self.communicate_default
        else:
            communicate = functools.partial(callback, self)
        return await (communicate(pool) if pool else communicate())

    # noinspection PyUnusedLocal
    async def communicate_default(self, pool: HandlerPool | None = None):
        """
        A single frame in the connection life.
        This method is intended to interact with the connection I/O through write() or/and read().
        It's called continuously by an external loop, and it can be stopped by kill().

        Parameters
        ----------
        pool : HandlerPool or None
            Connection pool instance that requested validation.
        """
        await self.data_loop()

    @classmethod
    def communication_backend(cls, endpoint_class: type[Endpoint], method=None):
        """
        Register communicate() implementation for given endpoint class (type).

        Parameters
        ----------
        endpoint_class
        method
        """
        if method is None:
            return functools.partial(cls.communication_backend, endpoint_class)
        cls._communication_backends.register(endpoint_class, lambda _: method)
        return method

    @classmethod
    def validation_backend(cls, endpoint_class: type[Endpoint], method=None):
        """
        Register validate() implementation for given endpoint class (type).

        Parameters
        ----------
        endpoint_class
        method
        """
        if method is None:
            return functools.partial(cls.validation_backend, endpoint_class)
        cls._validation_backends.register(endpoint_class, lambda _: method)
        return method

    # noinspection PyUnusedLocal
    def on_queue_overflow(self, *args: Any):
        """Called when the datagram queue overflows."""
        self.stop()

    def on_invalid_data(self, data: bytearray, *args: Any):
        """Called when the incoming data did not pass validation."""

    async def handle_data(self, data: bytes, *args: Any):
        """Called when a new valid data has just been dequeued."""

    async def data_loop(self, data_handler=None):
        if data_handler is None:
            data_handler = self.handle_data
        loop = asyncio.get_running_loop()
        while self.is_alive:
            task = loop.create_task(data_handler(*await self.queue.get()))
            self.future.add_done_callback(task.cancel)

    def on_data(self, data: bytearray, *args: Any):
        original = data.copy()
        if self.endpoint.validate_data(data, *args):
            try:
                self.queue.put_nowait((bytes(data), *args))
            except asyncio.QueueFull:
                # Potential DoS/DDoS attack
                self.on_queue_overflow(*args)
        else:
            self.on_invalid_data(original, *args)

    def __init_subclass__(cls):
        default_dispatch = (lambda _: NotImplemented)
        cls._communication_backends = staticmethod(functools.singledispatch(default_dispatch))
        cls._validation_backends = staticmethod(functools.singledispatch(default_dispatch))
        for _, method in inspect.getmembers(cls):
            communicates_as_endpoint_class = getattr(method, cls.COMMUNICATION_BACKEND_FLAG, None)
            if communicates_as_endpoint_class:
                cls.communication_backend(communicates_as_endpoint_class, method)
            validates_as_endpoint_class = getattr(method, cls.VALIDATION_BACKEND_FLAG, None)
            if validates_as_endpoint_class:
                cls.validation_backend(validates_as_endpoint_class, method)


class ConnectionHandler(BaseEndpointHandler):
    def __init__(
        self,
        future: asyncio.Future,
        endpoint: Endpoint,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        super().__init__(future, endpoint)
        self.reader = reader
        self.writer = writer
        sock = self.writer.get_extra_info('socket')
        (local_host, local_port, *_), (host, port, *_) = sock.getsockname(), sock.getpeername()
        self._host = ipaddress.ip_address(host)
        self._local_host = ipaddress.ip_address(local_host)
        self._port = port
        self._local_port = local_port
        self._domain = socket.getfqdn(self._host.compressed)
        self._local_domain = socket.getfqdn(self._local_host.compressed)

    async def on_sync(self, pool: HandlerPool, data: str | bytes):
        await self.write(data)

    @functools.singledispatchmethod
    async def write(self, data: str | bytes):
        """Send data through the connection, bytes or string."""
        warnings.warn(f'unknown write() data type {type(data).__name__}, defaulting to message()')
        await self.message(data)

    @write.register(bytes)
    async def send(self, data: bytes):
        self.writer.write(data)
        await self.writer.drain()

    @write.register(str)
    async def message(self, data: str):
        await self.send(data.encode(self.MSG_ENCODING))

    async def read(self, n=-1):
        return await self.handle_read(await self.reader.read(n))

    async def read_exactly(self, n):
        try:
            data = await self.reader.readexactly(n)
        except asyncio.IncompleteReadError as exc:
            data = exc.partial
            missing = exc.expected - len(data)
        else:
            missing = 0
        return await self.handle_read(data, missing)

    async def read_until(self, sep):
        return await self.handle_read(await self.reader.readuntil(sep))

    async def handle_read(self, data, missing=0):
        if not missing:
            if not data:  # stream EOF
                self.stop()
        return data

    def _stop(self):
        self.writer.close()

    def __init_subclass__(cls):
        super().__init_subclass__()
        if isinstance(cls.write, functools.singledispatchmethod):
            write_meth = typing.cast(functools.singledispatchmethod, cls.write)
            if cls.write is ConnectionHandler.write:
                write_meth = functools.singledispatchmethod(write_meth.func)
            for func in write_meth.dispatcher.registry.values():
                write_meth.register(getattr(cls, func.__name__))


class DatagramProtocol(asyncio.DatagramProtocol):
    handler: type[DatagramEndpointHandler] | None = None

    def set_handler(self, handler):
        self.handler = handler

    def datagram_received(self, datagram, addr) -> None:
        if not self.handler:
            return
        self.handler.on_data(bytearray(datagram))

    def error_received(self, exc) -> None:
        if not self.handler:
            return

    def connection_made(self, transport) -> None:
        if not self.handler:
            return
        self.handler.transport = transport

    def connection_lost(self, exc) -> None:
        if not self.handler:
            return
        self.handler.connection_lost(exc)


class DatagramEndpointHandler(BaseEndpointHandler):
    def __init__(
        self,
        future: asyncio.Future,
        endpoint: Endpoint,
        transport: asyncio.DatagramTransport,
        protocol: DatagramProtocol
    ):
        # https://github.com/python/cpython/issues/91227
        if os.name == 'nt':
            loop = asyncio.SelectorEventLoop()
            asyncio.set_event_loop(loop)
        super().__init__(future, endpoint)
        self.future.add_done_callback(transport.abort)
        self.transport = transport
        self.protocol = protocol
        self.protocol.set_handler(self)

    def connection_lost(self, exc: Exception | None):
        if exc:
            self.future.set_exception(exc)
        else:
            self.future.set_result(None)
        self.transport = None

    def _stop(self):
        if self.transport:
            self.transport.close()

    async def on_sync(self, pool, datagram):
        if self.transport:
            await self.write_to(datagram)

    @functools.singledispatchmethod
    async def write_to(self, datagram: str | bytes, addr: _AddressT | None = None):
        """Send data through the endpoint, bytes or string."""
        warnings.warn(
            f'unknown write_to() datagram type {type(datagram).__name__}, '
            'defaulting to message_to()'
        )
        await self.message_to(datagram, addr)

    @write_to.register(bytes)
    async def send_to(self, datagram, addr=None):
        self.transport.sendto(datagram, addr)

    @write_to.register(str)
    async def message_to(self, datagram, addr=None):
        await self.send_to(datagram.encode(self.MSG_ENCODING), addr)


def communication_backend(endpoint_class: type[Endpoint], method=None):
    if method is None:
        return functools.partial(communication_backend, endpoint_class)
    setattr(method, BaseEndpointHandler.COMMUNICATION_BACKEND_FLAG, endpoint_class)
    return method


def validation_backend(endpoint_class: type[Endpoint], method=None):
    if method is None:
        return functools.partial(validation_backend, endpoint_class)
    setattr(method, BaseEndpointHandler.VALIDATION_BACKEND_FLAG, endpoint_class)
    return method


class HandlerPool:
    """
    Handler pool that stores all the handlers and also may keep them alive.
    Use it to impose custom behavior on all of them across many various servers,
    e.g. for setting timeout on each, but also for storing connections
    of various endpoint endpoint_classs (server connections, client connections etc.).
    """

    def __init__(self, future: asyncio.Future | None = None):
        self.future = future
        self._future_bound = False
        self.handlers = weakref.WeakSet()

    def bind_future(self, future=None):
        if future is None:
            if self._future_bound:
                return
            loop = asyncio.get_event_loop()
            future = loop.create_future()
        self.future = future
        self._future_bound = True

    async def sync(
            self,
            data: str | bytes,
            *args: Any,
            exclude_handlers: list[BaseEndpointHandler] | None = None
    ):
        handlers = self.handlers[:]
        if exclude_handlers:
            for excluded_handler in exclude_handlers:
                handlers.discard(excluded_handler)
        await asyncio.gather(*(
            handler.on_sync(self, data, *args)
            for handler in handlers
        ))

    def end(self):
        self.future.set_result(None)

    async def run(self, handler):
        if not handler.is_alive:
            return

        self.bind_future()
        self.handlers.add(handler)
        communicate = (
            handler.communicate if handler.endpoint.pool is self
            else functools.partial(handler.communicate, self)
        )

        while handler.is_alive and not (self.future.done() or self.future.cancelled()):
            try:
                await communicate()
            except Exception:
                await handler.endpoint.on_error()
                handler.stop()
            finally:
                if not handler.is_alive:
                    self.handlers.remove(handler)
                    if not self.handlers:
                        self.end()
                    break

    def task(self, task: typing.Coroutine):
        loop = asyncio.get_running_loop()
        if isinstance(task, typing.Coroutine):
            task = loop.create_task(task)
        self.bind_future()
        if self.future:
            self.future.add_done_callback(task.cancel)

    async def on_endpoint(
        self,
        endpoint: Endpoint,
        *args, **kwargs
    ):
        loop = asyncio.get_running_loop()
        handler = endpoint.create_handler(loop.create_future(), *args, **kwargs)
        await handler.validate()
        await self.run(handler)

    def connection_callback(self, endpoint, *args):
        if not args:
            return functools.partial(self.connection_callback, endpoint)
        return self.task(self.on_endpoint(endpoint, *args))


class Endpoint:
    """
    A class that describes connection endpoint (client/server, peer) and stores related
    connections. It works with ConnectionPool, which manages to keep those connections alive
    by calling proper connection's methods in asynchronous loop.

    To start an endpoint, use start() method.
    You can set default host and port as class attributes.
    """

    default_host: str | None = None
    default_port: int | None = None
    handler_class: type[BaseEndpointHandler] = ConnectionHandler
    pool_class: type[HandlerPool] = HandlerPool
    error_report_mutex: asyncio.Lock = asyncio.Lock()

    def __init__(
        self,
        pool: HandlerPool | None = None,
        wrapper_class: type[ConnectionHandler] | None = None,
        handler_kwargs: dict | None = None,
        **endpoint_kwargs: Any
    ):
        if pool is None:
            pool = self.pool_class()
        self.pool = pool

        self.handlers: list[wrapper_class] = []
        self.endpoint_kwargs = endpoint_kwargs
        self.handler_kwargs = handler_kwargs or {}

        if wrapper_class:
            self.handler_class = wrapper_class

    @property
    def is_ssl(self) -> bool:
        return self.endpoint_kwargs.get('ssl_context') is not None

    def create_handler(
            self,
            future: asyncio.Future,
            *args: Any, **kwargs: Any
    ) -> handler_class:
        handler = self.handler_class(future, self, *args, **kwargs)
        handler.__post_init__(**self.handler_kwargs)
        self.handlers.append(handler)
        return handler

    def start(
        self,
        setup: Callable[[Endpoint], ...] | None = None,
        blocking: bool = True,
        setup_timeout: _TimeoutT = None,
        timeout: _TimeoutT = None
    ):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        task = None
        if callable(setup):
            setup_ret = setup(self)
            if inspect.isawaitable(setup_ret):
                if isinstance(setup_ret, asyncio.Future):
                    task = setup_ret
                else:
                    task = loop.create_task(setup_ret)
        if task:
            loop.run_until_complete(asyncio.wait_for(task, timeout=setup_timeout))
        if blocking:
            futs = [handler.future for handler in self.handlers]
            if futs:
                loop.run_until_complete(asyncio.wait(futs, timeout=timeout))

    def start_concurrent(
        self,
        cls: type,
        setup: Callable[[Endpoint], ...] | None = None,
        blocking: bool = True,
        setup_timeout: _TimeoutT = None,
        timeout: _TimeoutT = None
    ):
        concurrent = cls(
            target=self.start,
            kwargs=dict(
                setup=setup,
                setup_timeout=setup_timeout,
                blocking=True
            )
        )
        concurrent.start()
        if blocking:
            concurrent.join(timeout)
        return concurrent

    def start_thread(
        self,
        setup: Callable[[Endpoint], ...] | None = None,
        blocking: bool = True,
        setup_timeout: _TimeoutT = None,
        timeout: _TimeoutT = None
    ):
        """Start an endpoint in a separate thread."""
        return self.start_concurrent(
            threading.Thread, setup,
            blocking=blocking,
            setup_timeout=setup_timeout,
            timeout=timeout
        )

    def start_process(
        self,
        setup: Callable[[Endpoint], ...] | None = None,
        blocking: bool = True,
        setup_timeout: _TimeoutT = None,
        timeout: _TimeoutT = None
    ):
        """Start an endpoint in a separate process."""
        return self.start_concurrent(
            multiprocessing.Process, setup,
            blocking=blocking,
            setup_timeout=setup_timeout,
            timeout=timeout
        )

    def stop(self):
        """Stop all connections bound to this endpoint."""
        handlers = self.handlers
        if handlers:
            for handler in handlers:
                handler.stop()
            handlers.clear()

    def validate_data(self, data: bytearray):
        return True

    async def begin(
        self,
        host: str | None = None,
        port: int | None = None,
        **endpoint_kwargs: Any
    ):
        """
        Create and return connected asynchronous endpoint(s). Could be a server, client, peer etc.

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
        return await self.begin_endpoint(host, port, **endpoint_kwargs)

    async def begin_endpoint(
        self,
        host: str,
        port: int,
        **endpoint_kwargs: Any
    ):
        """Create and return the asynchronous endpoint protocol. Could be a server, client etc."""
        raise NotImplementedError

    @classmethod
    def handler(cls, handler_class: type[ConnectionHandler]):
        cls.handler_class = handler_class
        return handler_class

    @classmethod
    def set_pool_class(cls, pool_class: type[HandlerPool]):
        cls.pool_class = pool_class
        return pool_class

    async def on_error(self):
        async with self.error_report_mutex:
            print(f'Exception in {type(self).__name__}:', file=sys.stderr)
            traceback.print_exc()


class TCPServer(Endpoint):
    default_host = '127.0.0.1'

    async def begin_endpoint(self, host, port, **endpoint_kwargs):
        endpoint_kwargs.update(host=host, port=port)
        return await asyncio.start_server(self.pool.connection_callback(self), **endpoint_kwargs)

    async def start_server(
            self,
            host: str | None = None,
            port: int | None = None,
            **endpoint_kwargs: Any
    ):
        """Start a server."""
        return await self.begin(host, port, **endpoint_kwargs)


class TCPClient(Endpoint):
    async def begin_endpoint(self, host, port, **endpoint_kwargs):
        endpoint_kwargs.update(host=host, port=port)
        try:
            reader, writer = await asyncio.open_connection(**endpoint_kwargs)
        except Exception:
            await self.on_error()
            return
        self.pool.connection_callback(self, reader, writer)
        return reader, writer

    async def connect(
            self,
            host: str | None = None,
            port: int | None = None,
            **endpoint_kwargs: Any
    ):
        """Connect to a server."""
        return await self.begin(host, port, **endpoint_kwargs)


class UDPEndpoint(Endpoint):
    use_local_addr: ClassVar[bool]
    protocol_class: type[DatagramProtocol] = DatagramProtocol

    async def begin_endpoint(self, host, port, **endpoint_kwargs):
        endpoint_kwargs.update(protocol_factory=self.protocol_class)
        addr = (host, port)
        if self.use_local_addr:
            endpoint_kwargs.update(local_addr=addr)
        else:
            endpoint_kwargs.update(remote_addr=addr)
        loop = asyncio.get_running_loop()
        try:
            transport, protocol = await loop.create_datagram_endpoint(**endpoint_kwargs)
        except Exception:
            await self.on_error()
            return
        self.pool.connection_callback(self, transport, protocol)
        return transport


class UDPClient(UDPEndpoint):
    use_local_addr = False


class UDPServer(UDPEndpoint):
    use_local_addr = True
    default_host = '127.0.0.1'


# Recipes
def start_client(client, setup, setup_timeout=None, timeout=None):
    """Start a client and suppress the timeout error."""
    try:
        client.start(setup, setup_timeout=setup_timeout, timeout=timeout)
    except asyncio.TimeoutError:
        pass
    finally:
        client.stop()


def start_race(
        client: TCPClient, 
        *addresses: _AddressT,
        setup_timeout: _TimeoutT = None, 
        timeout: _TimeoutT = None
):
    """
    Start a connection race.

    Parameters
    ----------
    client : TCPClient
    addresses : _AddressT
    setup_timeout : int or float or None
    timeout : int or float or None

    Returns
    -------
    client
    """
    if not addresses:
        addresses = [[]]
    start_client(
        client,
        lambda _: asyncio.gather(*(client.connect(*address) for address in addresses)),
        setup_timeout=setup_timeout, timeout=timeout
    )
    return client
