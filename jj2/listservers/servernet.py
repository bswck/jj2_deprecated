import dataclasses
import json
import typing

import blinker
from loguru import logger

from jj2.networking.server import Connection, Server
from jj2.listservers import db


@dataclasses.dataclass
class Request:
    action: str
    data: typing.Iterable
    origin: str

    def validate(self):
        if not all((
            isinstance(self.action, str),
            isinstance(self.data, typing.Iterable),
            isinstance(self.origin, str)
        )):
            raise ValueError('invalid ServerNet RPC request')


class ServerNetConnection(Connection):
    rpc = blinker.Namespace()
    parse = json.loads
    max_parse_attempts = 11
    payload_keys = {'action', 'data', 'origin'}
    request_class = Request

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.request = None

    async def validate(self):
        if self.server.is_ssl:
            if not self.is_localhost:
                logger.warning(f'Outside IP {self.ip} tried connection to remote admin API')
                self.kill()
        elif (
            self.ip not in db.get_mirrors()
            or self.ip.is_localhost
        ):
            logger.warning(f'Unauthorized ServerNet connection from {self.ip}:{self.server.port}')
            self.kill()
        db.update_lifesign(self.ip)

    def try_read(self):
        try:
            data = self.recv(2048)
        except OSError:
            logger.error(
                f'ServerNet connection from {self.address} timed out while receiving data'
            )
            return self.kill()
        else:
            self.buffer.extend(data)
        self.read_attempts += 1

    def try_parse_payload(self):
        payload = None
        if not self.request:
            try:
                payload = self.parse(self.buffer.decode(self.msg_encoding, 'ignore'))
            except ValueError:
                pass
        return payload

    def cast(self, payload):
        if self.buffer and not payload:
            logger.error(
                f'ServerNet update received from {self.ip}, '
                f'but could not acquire valid payload (got {self.buffer})'
            )
            return self.kill()

        if not all(payload.get(key) for key in self.payload_keys):
            logger.error(
                f'ServerNet update received from {self.ip}, but JSON was incomplete'
            )
            return self.kill()

        if payload['origin'].strip() == self.server.ip:
            return self.kill()

        request = self.request(**payload)
        try:
            request.validate()
        except ValueError:
            logger.error(
                f'ServerNet update received from {self.ip}, but the data was incorrect'
            )
            return self.kill()

        self.request = request

    async def serve(self):
        self.try_read()
        payload = self.try_parse_payload()
        if self.read_attempts > self.max_parse_attempts:
            return self.kill()
        if payload:
            self.cast(payload)
        if self.request:
            self.handle_request()

    def handle_request(self):
        sender = type(self)
        action = self.request.action
        for chunk in map(map_request_chunk, self.request.data):
            self.rpc.signal(action).send(sender, **chunk)


def map_request_chunk(chunk):
    chunk['server_id'] = chunk.get('id')
    return chunk


class ServerNetRPC(Server):
    default_port = 10058
    connection_class = ServerNetConnection


def servernet_command(command_name):
    return (
        ServerNetConnection
        .rpc.signal(command_name)
        .connect
    )


@servernet_command('server')
def on_server(
    connection, 
    server_id=None,
    **traits
):
    if server_id is None:
        logger.error(
            f'Received incomplete server data '
            f'from ServerNet connection {connection.ip}'
        )
        return
    traits.update(remote=1)
    db.update_server(server_id, **traits)


@servernet_command('add-banlist')
def on_add_banlist():
    pass


@servernet_command('delete-banlist')
def on_delete_banlist():
    pass


@servernet_command('delist')
def on_delist():
    pass


@servernet_command('add-mirror')
def on_add_mirror():
    pass


@servernet_command('delete-mirror')
def on_delete_mirror():
    pass


@servernet_command('send-motd')
def on_set_motd():
    pass


@servernet_command('request')
def on_request():
    pass


@servernet_command('hello')
def on_hello():
    pass


@servernet_command('request-log')
def on_request_log():
    pass


@servernet_command('request-log-from')
def on_request_log_from():
    pass


@servernet_command('send-log')
def on_send_log():
    pass


@servernet_command('reload')
def on_reload():
    pass


@servernet_command('get-servers')
def on_get_servers():
    pass


@servernet_command('get-banlist')
def on_get_banlist():
    pass


@servernet_command('get-motd')
def on_get_motd():
    pass


@servernet_command('get-motd-expires')
def on_get_motd_expires():
    pass


@servernet_command('get-mirrors')
def on_get_mirrors():
    pass


@servernet_command('ping')
def on_ping():
    pass
