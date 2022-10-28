import contextvars
import dataclasses
import functools
import inspect
import json
import typing

import blinker
from loguru import logger

from jj2.listservers import db
from jj2.aiolib import AsyncConnection, AsyncServer


@dataclasses.dataclass
class RPC:
    action: str
    data: typing.Iterable
    origin: str

    def validate(self):
        if not (
            isinstance(self.action, str)
            and isinstance(self.data, typing.Iterable)
            and isinstance(self.origin, str)
        ):
            raise ValueError('invalid ServerNet RPC request')


class ServerNetConnection(AsyncConnection):
    rpc_ns = blinker.Namespace()
    rpc_class = RPC
    _rpc_obj = contextvars.ContextVar('_rpc_obj', default=None)
    _rpc_ctx = contextvars.ContextVar('_rpc_ctx')
    _sync_fg = contextvars.ContextVar('_sync_fg', default=None)

    decode_payload = staticmethod(json.loads)
    encode_payload = staticmethod(json.dumps)

    MAX_READ_ATTEMPTS = 11
    PAYLOAD_KEYS = {'action', 'data', 'origin'}
    UNSYNCED_REQS = {'hello', 'request', 'delist', 'request-log', 'send-log', 'request-log-from'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.sync_chunks = set()

    async def validate(self):
        if self.server.is_ssl:
            if not self.is_localhost:
                logger.warning(f'Outside IP {self.ip} tried connection to remote admin API')
                self.kill()
        elif (
            self.ip not in db.get_mirrors().values()
            or self.is_localhost
        ):
            logger.warning(f'Unauthorized ServerNet connection from {self.ip}:{self.server.port}')
            self.kill()
        db.update_lifesign(self.ip)

    @property
    def ctx(self):
        try:
            ctx = self._rpc_ctx.get() or {}
        except LookupError:
            self.ctx = ctx = {}
        return ctx

    @ctx.setter
    def ctx(self, data):
        self._rpc_ctx.set(data)

    @property
    def rpc(self) -> RPC | None:
        return self._rpc_obj.get()

    @rpc.setter
    def rpc(self, rpc):
        self._rpc_obj.set(rpc)

    @property
    def can_sync(self):
        return (
            self.sync_chunks
            and self.rpc
            and self.rpc.action not in self.UNSYNCED_REQS
        )

    @property
    def sync_flag(self):
        return self._sync_fg.get()

    @sync_flag.setter
    def sync_flag(self, sync_flag):
        self._sync_fg.set(sync_flag)

    def attempt_read(self):
        try:
            data = self.recv(2048)
        except OSError:
            logger.error(
                f'ServerNet connection from {self.address} broke while receiving data'
            )
            return self.kill()
        else:
            self.buffer.extend(data)
        self.read_attempts += 1

    def attempt_decode(self):
        payload = None
        if not self.rpc:
            try:
                payload = self.decode_payload(self.buffer.decode(self.MSG_ENCODING, 'ignore'))
            except ValueError:
                pass
        return payload

    def make_rpc(self, payload):
        if self.buffer and not payload:
            logger.error(
                f'ServerNet update received from {self.ip}, '
                f'but could not acquire valid payload (got {self.buffer})'
            )
            return self.kill()

        if not all(payload.get(key) for key in self.PAYLOAD_KEYS):
            logger.error(
                f'ServerNet update received from {self.ip}, but JSON was incomplete'
            )
            return self.kill()

        if payload['origin'].strip() == self.server.ip:
            return self.kill()

        rpc = self.rpc_class(**payload)
        try:
            rpc.validate()
        except ValueError:
            logger.error(
                f'ServerNet update received from {self.ip}, but the data was incorrect'
            )
            return self.kill()

        self.rpc = rpc

    async def run(self):
        self.attempt_read()
        payload = self.attempt_decode()
        if self.read_attempts > self.MAX_READ_ATTEMPTS:
            return self.kill()
        if payload:
            self.make_rpc(payload)
        if self.rpc:
            try:
                self.dispatch_rpc()
            finally:
                self.kill()

    def dispatch_rpc(self):
        sender = type(self)
        action_name = self.rpc.action
        for chunk in self.ctx:
            signal = self.rpc_ns.signal(action_name)
            signal.send(sender, **chunk)
        if self.sync_chunks:
            self.sync_rpc()

    def sync_rpc(self):
        if not self.can_sync:
            return
        if self.can_sync:
            rpc = self.rpc_class(
                action=self.rpc.action,
                data=list(self.sync_chunks),
                origin=self.ip
            )
            payload = self.encode_payload(rpc)
            self.sync(payload)


class ServerNet(AsyncServer):
    default_port = 10058
    connection_class = ServerNetConnection


def rename(**old_to_new):
    return lambda chunk: {
        old_to_new.get(key, key): value
        for key, value in chunk.items()
    }


_ORIG_FUNC_ATTR = 'func'
_PASS_CONTEXT_ARG = 'ctx'

_FUNC_MISSING = object()


def callback(action, mappers=None, func=_FUNC_MISSING, *, sync=None):
    def callback_decorator(decorated_func):
        nonlocal func
        if not decorated_func:
            func = decorated_func
        parameters = inspect.signature(func).parameters
        has_variadic_kw = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        @functools.wraps(func)
        def callback_wrapper(connection, /, **ctx):
            compat_action = connection.rpc.action == action

            if compat_action:
                connection.ctx = ctx
                ctx_mappers = mappers if isinstance(mappers, typing.Iterable) else [mappers]
                for mapper in filter(callable, ctx_mappers):
                    ctx = mapper(ctx)

            if not has_variadic_kw:
                ctx = {param: value for param, value in ctx.items() if param in parameters}

            if _PASS_CONTEXT_ARG in parameters:
                ctx[_PASS_CONTEXT_ARG] = connection.ctx

            if sync is not None:
                connection.sync_flag = sync
            func(connection, **ctx)
            do_sync = connection.sync_flag

            if compat_action and do_sync:
                updated_ctx = connection.ctx
                connection.sync_chunks.add(updated_ctx)

            return do_sync

        wrapper = (
            ServerNetConnection
            .rpc_ns.signal(action)
            .connect(callback_wrapper)
        )
        setattr(
            wrapper, _ORIG_FUNC_ATTR,
            getattr(func, _ORIG_FUNC_ATTR, func)
        )
        return wrapper

    if func is None:
        return callback_decorator(lambda connection: None)
    func = None
    return callback_decorator


def call(func, *args, **kwargs):
    func = getattr(func, _ORIG_FUNC_ATTR, func)
    return func(*args, **kwargs)


@callback('server', rename(id='server_id'), sync=True)
def on_server(connection, ctx, server_id=None):
    if server_id is None:
        logger.error(
            f'Received incomplete server data '
            f'from ServerNet connection {connection.ip}'
        )
        connection.sync_flag = False
        return
    ctx.update(remote=1)
    db.update_server(server_id, **ctx)


@callback('add-banlist', sync=True)
def on_add_banlist(connection, ctx):
    ctx.setdefault(origin=connection.server.ip)
    db.add_banlist_entry(**ctx)
    logger.info(f'Added banlist entry via ServerNet connection {connection.ip}')


@callback('delete-banlist', sync=True)
def on_delete_banlist(connection, ctx):
    ctx.setdefault(origin=connection.server.ip)
    db.delete_banlist_entry(**ctx)
    logger.info(f'Removed banlist entry via ServerNet connection {connection.ip}')


@callback('delist', rename(id='server_id'), sync=True)
def on_delist(connection, server_id):
    server = db.get_server(server_id)
    if server:
        if server.remote:
            db.delete_server(server_id)
        else:
            logger.error(
                f'Mirror {connection.ip} tried to delist server {server_id}, '
                f'but the server was not remote!'
            )
            connection.sync_flag = False
    else:
        logger.error(
            f'Mirror {connection.ip} tried to delist server {server_id}, '
            f'but the server was unknown'
        )
        connection.sync_flag = False


@callback('add-mirror')
def on_add_mirror(connection):
    pass


@callback('delete-mirror')
def on_delete_mirror(connection):
    pass


@callback('send-motd')
def on_set_motd(connection):
    pass


@callback('request')
def on_request(connection):
    pass


@callback('hello')
def on_hello(connection):
    call(on_request, connection)


@callback('request-log')
def on_request_log(connection):
    pass


@callback('request-log-from')
def on_request_log_from(connection):
    pass


@callback('send-log')
def on_send_log(connection):
    pass


@callback('reload')
def on_reload(connection):
    pass


@callback('get-servers')
def on_get_servers(connection):
    pass


@callback('get-banlist')
def on_get_banlist(connection):
    pass


@callback('get-motd')
def on_get_motd(connection):
    pass


@callback('get-motd-expires')
def on_get_motd_expires(connection):
    pass


@callback('get-mirrors', sync=True)
def on_get_mirrors(connection):
    mirrors = db.get_mirrors()
    payload = connection.encode_payload([
        dict(name=name, address=address)
        for name, address in mirrors
    ])
    connection.msg(payload)


callback('ping', func=None, sync=False)
