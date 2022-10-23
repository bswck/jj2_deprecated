import dataclasses
import json
import typing
import weakref

import blinker
from loguru import logger

from jj2.listservers import db
from jj2.networking import Connection, AsyncServer


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


class ServerNetConnection(Connection):
    rpc = blinker.Namespace()
    rpc_class = RPC

    decode_payload = staticmethod(json.loads)
    encode_payload = staticmethod(json.dumps)

    MAX_READ_ATTEMPTS = 11
    PAYLOAD_KEYS = {'action', 'data', 'origin'}
    UNSYNCED_REQS = {"hello", "request", "delist", "request-log", "send-log", "request-log-from"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.rpc = None
        self.sync_chunks = weakref.WeakSet()

    async def validate(self):
        if self.server.is_ssl:
            if not self.is_localhost:
                logger.warning(f'Outside IP {self.ip} tried connection to remote admin API')
                self.kill()
        elif (
            self.ip not in db.get_mirrors().values()
            or self.ip.is_localhost
        ):
            logger.warning(f'Unauthorized ServerNet connection from {self.ip}:{self.server.port}')
            self.kill()
        db.update_lifesign(self.ip)

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
                payload = self.decode_payload(self.buffer.decode(self.msg_encoding, 'ignore'))
            except ValueError:
                pass
        return payload

    def deduce_rpc(self, payload):
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
            self.deduce_rpc(payload)
        if self.rpc:
            try:
                self.dispatch_rpc()
            finally:
                self.kill()

    def dispatch_rpc(self):
        sender = type(self)
        action_name = self.rpc.action
        for chunk in self.rpc.data:
            signal = self.rpc.signal(action_name)
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


def renamed_params(**arg_map):
    return lambda chunk: {
        arg_map.get(key, key): value
        for key, value in chunk
    }


def action(command_name, chunk_mapper=None):
    def command_decorator(func):
        def func_wrapper(connection, /, _map_args=True, **chunk):
            if callable(chunk_mapper):
                chunk = chunk_mapper(chunk)
            should_sync = func(connection, **chunk)
            if should_sync:
                connection.sync_chunks.add(chunk)
            return should_sync

        wrapper = (
            ServerNetConnection
            .rpc.signal(command_name)
            .connect(func_wrapper)
        )
        wrapper.func = func
        return wrapper

    return command_decorator


@action('server', renamed_params(server_id='id'))
def on_server(connection, server_id=None, **data):
    if server_id is None:
        logger.error(
            f'Received incomplete server data '
            f'from ServerNet connection {connection.ip}'
        )
        return False
    data.update(remote=1)
    db.update_server(server_id, **data)
    return True


@action('add-banlist', renamed_params(banlist_type='type'))
def on_add_banlist(connection, address, banlist_type, note, origin, reserved):
    pass


@action('delete-banlist')
def on_delete_banlist(connection):
    pass


@action('delist')
def on_delist(connection):
    pass


@action('add-mirror')
def on_add_mirror(connection):
    pass


@action('delete-mirror')
def on_delete_mirror(connection):
    pass


@action('send-motd')
def on_set_motd(connection):
    pass


@action('request')
def on_request(connection):
    pass


@action('hello')
def on_hello(connection):

    on_request.func(connection)


@action('request-log')
def on_request_log(connection):
    pass


@action('request-log-from')
def on_request_log_from(connection):
    pass


@action('send-log')
def on_send_log(connection):
    pass


@action('reload')
def on_reload(connection):
    pass


@action('get-servers')
def on_get_servers(connection):
    pass


@action('get-banlist')
def on_get_banlist(connection):
    pass


@action('get-motd')
def on_get_motd(connection):
    pass


@action('get-motd-expires')
def on_get_motd_expires(connection):
    pass


@action('get-mirrors')
def on_get_mirrors(connection):
    mirrors = db.get_mirrors()
    payload = connection.encode_payload([
        dict(name=name, address=address)
        for name, address in mirrors
    ])
    connection.msg(payload)
    return True


@action('ping')
def on_ping(_connection):
    return False
