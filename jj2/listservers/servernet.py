import contextvars
import dataclasses
import inspect
import json
import typing

import blinker
from loguru import logger

from jj2.classes import BanlistEntry
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
    _rpc_var = contextvars.ContextVar('_rpc_var', default=None)
    _rpc_chunk = contextvars.ContextVar('_rpc_chunk', default={})

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
        return self._rpc_chunk.get()

    @ctx.setter
    def ctx(self, data):
        self._rpc_chunk.set(data)

    @property
    def rpc(self) -> RPC | None:
        return self._rpc_var.get()

    @rpc.setter
    def rpc(self, rpc):
        self._rpc_var.set(rpc)

    @property
    def should_sync(self):
        return (
            self.sync_chunks
            and self.rpc
            and self.rpc.action not in self.UNSYNCED_REQS
        )

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
        if not self.should_sync:
            return
        if self.should_sync:
            req = self.rpc_class(
                action=self.rpc.action,
                data=list(self.sync_chunks),
                origin=self.ip
            )
            msg = self.encode_payload(req)
            self.sync(msg)


class ServerNet(AsyncServer):
    default_port = 10058
    connection_class = ServerNetConnection


def rename_params(**arg_map):
    return lambda chunk: {
        arg_map.get(key, key): value
        for key, value in chunk
    }


def gather_to_obj(cls, arg_name, *arg_list, **arg_mappers):
    if not (*arg_list, *arg_mappers):
        arg_list = list(inspect.signature(cls).parameters)
    return lambda chunk: {
        arg_name: cls(**{
            arg: (mapper(arg) if mapper else arg)
            for arg, mapper
            in {**dict.fromkeys(arg_list, None), **arg_mappers}
        })
    }


def gather(**args_to_objects):
    return [
        gather_to_obj(
            (cls_args := cls if isinstance(cls, list) else [cls])[0],
            arg_name, *cls_args[1:] if cls_args else []
        )
        for arg_name, cls
        in args_to_objects.items()
    ]


_ORIG_FUNC_ATTR = 'func'


def callback(command_name, mappers=None):
    def command_decorator(func):
        def func_wrapper(connection, /, **ctx):
            connection.ctx = ctx
            arg_mappers = mappers if isinstance(mappers, typing.Iterable) else [mappers]
            for mapper in filter(callable, arg_mappers):
                ctx = mapper(ctx)
            should_sync = func(connection, **ctx)
            if should_sync:
                updated_ctx = connection.ctx
                connection.sync_chunks.add(updated_ctx)
            return should_sync

        wrapper = (
            ServerNetConnection
            .rpc_ns.signal(command_name)
            .connect(func_wrapper)
        )
        setattr(wrapper, _ORIG_FUNC_ATTR, func)
        return wrapper

    return command_decorator


def unwrap_callback(func):
    while unwrapped := getattr(func, _ORIG_FUNC_ATTR, None):
        func = unwrapped
    return func


def call_unsynced(cb, *args, **kwargs):
    return unwrap_callback(cb)(*args, **kwargs)


@callback('server', rename_params(server_id='id'))
def on_server(connection, server_id=None):
    if server_id is None:
        logger.error(
            f'Received incomplete server data '
            f'from ServerNet connection {connection.ip}'
        )
        return False
    connection.ctx.update(remote=1)
    db.update_server(server_id, **connection.ctx)
    return True


@callback('add-banlist', gather(entry=BanlistEntry))
def on_add_banlist(connection, entry):
    pass


@callback('delete-banlist', gather(entry=BanlistEntry))
def on_delete_banlist(connection):
    pass


@callback('delist')
def on_delist(connection):
    pass


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
    call_unsynced(on_request, connection)


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


@callback('get-mirrors')
def on_get_mirrors(connection):
    mirrors = db.get_mirrors()
    payload = connection.encode_payload([
        dict(name=name, address=address)
        for name, address in mirrors
    ])
    connection.msg(payload)
    return True


@callback('ping')
def on_ping(_connection):
    return False
