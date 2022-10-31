import dataclasses
import functools
import inspect
import json
import typing

import blinker
from loguru import logger

from jj2.endpoints import Connection, Server
from jj2.listservers import db
from jj2.constants import LISTSERVER_RESERVED_MIRROR_NAME

if typing.TYPE_CHECKING:
    from typing import Any
    from typing import Callable


@dataclasses.dataclass
class Job:
    action: str
    data: typing.Iterable
    origin: str

    def validate(self):
        if not (
            isinstance(self.action, str)
            and isinstance(self.data, typing.Iterable)
            and isinstance(self.origin, str)
        ):
            raise ValueError('invalid ServerNet job request')


class ServerNetConnection(Connection):
    job_ns = blinker.Namespace()
    job_class = Job

    decode_payload = staticmethod(json.loads)
    encode_payload = staticmethod(json.dumps)

    MAX_READ_ATTEMPTS = 11
    PAYLOAD_KEYS = {'action', 'data', 'origin'}
    UNSYNCED_REQS = {'hello', 'request', 'delist', 'request-log', 'send-log', 'request-log-from'}

    def __init__(self, endpoint, reader, writer, mirror_pool):
        super().__init__(endpoint, reader, writer)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.sync_chunks = set()
        self.mirror_pool = mirror_pool
        self.sync_flag = None
        self.ctx = {}
        self.job = None

    async def validate(self, pool=None):
        if self.endpoint.is_ssl:
            if not self.is_localhost:
                logger.warning(f'Outside IP {self.address} tried connection to remote admin API')
                self.kill()
        elif (
            self.address not in db.read_mirrors().values()
            or self.is_localhost
        ):
            logger.warning(
                f'Unauthorized ServerNet connection from {self.address}:{self.endpoint.port}'
            )
            self.kill()
        db.update_mirror(self.address)

    @property
    def can_sync(self):
        return (
            self.sync_chunks
            and self.job
            and self.job.action not in self.UNSYNCED_REQS
        )

    def attempt_read(self):
        try:
            data = self.receive(2048)
        except OSError:
            logger.error(f'ServerNet connection from {self.address} broke while receiving data')
            return self.kill()
        else:
            self.buffer.extend(data)
        self.read_attempts += 1

    def attempt_decode(self):
        payload = None
        if not self.job:
            try:
                payload = self.decode_payload(self.buffer.decode(self.MSG_ENCODING, 'ignore'))
            except ValueError:
                pass
        return payload

    def make_job(self, payload):
        if self.buffer and not payload:
            logger.error(
                f'ServerNet update received from {self.address}, '
                f'but could not acquire valid payload (got {self.buffer})'
            )
            return self.kill()

        if not all(payload.get(key) for key in self.PAYLOAD_KEYS):
            logger.error(
                f'ServerNet update received from {self.address}, but JSON was incomplete'
            )
            return self.kill()

        if payload['origin'].strip() == self.endpoint.address:
            return self.kill()

        job = self.job_class(**payload)
        try:
            job.validate()
        except ValueError:
            logger.error(
                f'ServerNet update received from {self.address}, but the data was incorrect'
            )
            return self.kill()

        self.job = job

    async def run_once(self, pool=None):
        self.attempt_read()
        payload = self.attempt_decode()
        if self.read_attempts > self.MAX_READ_ATTEMPTS:
            return self.kill()
        if payload:
            self.make_job(payload)
        if self.job:
            try:
                self.dispatch_job()
            finally:
                self.kill()

    def dispatch_job(self):
        sender = type(self)
        action_name = self.job.action
        for ctx in self.ctx:
            signal = self.job_ns.signal(action_name)
            signal.send(sender, **ctx)
        if self.can_sync:
            self.sync_job()

    def request_job(self, action, *data, origin=None):
        if origin is None:
            origin = self.endpoint.address
        job = self.job_class(
            action=action,
            data=list(data),
            origin=origin
        )
        payload = self.encode_payload(job)
        self.write(payload)

    def sync_job(self):
        job = self.job_class(
            action=self.job.action,
            data=list(self.sync_chunks),
            origin=self.address
        )
        payload = self.encode_payload(job)
        self.sync(self.mirror_pool, payload)


class ServerNet(Server):
    default_port = 10058
    connection_class = ServerNetConnection


_ORIG_FUNC_ATTR = 'func'
_PASS_CONTEXT_ARG = 'ctx'
_FUNC_MISSING = object()


def servernet_job(
    action: str,
    func: 'Callable | object | None' = _FUNC_MISSING, *,
    args: 'set[str] | None' = None,
    sync: 'bool | None' = None
):
    """Register a ServerNet job function."""

    def job_decorator(decorated_func: Callable):
        nonlocal func
        if not decorated_func:
            func = decorated_func
        parameters = inspect.signature(func).parameters
        has_variadic_kw = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        @functools.wraps(func)
        def job_wrapper(connection: ServerNetConnection, /, **ctx: Any):
            compat_action = connection.job.action == action

            args_ok = True

            for arg in (args or set()):
                value = ctx.get(arg)
                if value is None:
                    logger.error(
                        f'Received incomplete server data '
                        f'from ServerNet connection {connection.address}'
                    )
                    connection.sync_flag = False
                    args_ok = False

            if args_ok:
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

        wrapper = (
            ServerNetConnection
            .job_ns.signal(action)
            .connect(job_wrapper)
        )
        setattr(
            wrapper, _ORIG_FUNC_ATTR,
            getattr(func, _ORIG_FUNC_ATTR, func)
        )
        return wrapper

    if func is None:
        return job_decorator(lambda connection: None)
    func = None
    return job_decorator


def servernet_forward(func, *args, **kwargs):
    func = getattr(func, _ORIG_FUNC_ATTR, func)
    return func(*args, **kwargs)


@servernet_job('server', args={'id'}, sync=True)
def on_server(connection, ctx):
    server_id = ctx['id']
    if server_id is None:
        logger.error(
            f'Received incomplete server data '
            f'from ServerNet connection {connection.address}'
        )
        connection.sync_flag = False
        return
    ctx.update(remote=1)
    db.update_server(server_id, **ctx)


@servernet_job('add-banlist', sync=True)
def on_add_banlist(connection, ctx):
    ctx.setdefault(origin=connection.endpoint.address)
    db.create_banlist_entry(**ctx)
    logger.info(f'Added banlist entry via ServerNet connection {connection.address}')


@servernet_job('delete-banlist', sync=True)
def on_delete_banlist(connection, ctx):
    ctx.setdefault(origin=connection.endpoint.address)
    db.delete_banlist_entry(**ctx)
    logger.info(f'Removed banlist entry via ServerNet connection {connection.address}')


@servernet_job('delist', args={'id'}, sync=True)
def on_delist(connection, ctx):
    server_id = ctx['id']
    server = db.read_server(server_id)
    if server:
        if server.remote:
            db.delete_server(server_id)
            logger.info(f'Delisted server via ServerNet connection {connection.address}')
        else:
            logger.error(
                f'Mirror {connection.address} tried to delist server {server_id}, '
                f'but the server was not remote!'
            )
            connection.sync_flag = False
    else:
        logger.error(
            f'Mirror {connection.address} tried to delist server {server_id}, '
            f'but the server was unknown'
        )
        connection.sync_flag = False


@servernet_job('add-mirror', args={'name', 'address'}, sync=True)
def on_add_mirror(connection, name, address):
    if db.read_mirror(name, address):
        logger.info(
            f'Mirror {connection.address} tried adding mirror {address}, '
            f'but name or address already known'
        )
        return
    if name == LISTSERVER_RESERVED_MIRROR_NAME:
        logger.error(
            f'{LISTSERVER_RESERVED_MIRROR_NAME} is a reserved name for mirrors, %s tried using it'
        )
        connection.sync_flag = False
        return
    db.create_mirror(name, address)
    connection.request_job('hello', {'from': connection.endpoint.address})
    logger.info(f'Added mirror {address} via ServerNet connection {connection.address}')


@servernet_job('delete-mirror', args={'name', 'address'}, sync=True)
def on_delete_mirror(connection, name, address):
    if not db.read_mirror(name, address):
        logger.info(f'Mirror {connection.address} tried to remove mirror {address}, but not known')
        return
    db.delete_mirror(name, address)
    logger.info(f'Deleted mirror {address} via ServerNet connection {connection.address}')


@servernet_job('send-motd')
def on_set_motd(connection):
    pass


@servernet_job('request')
def on_request(connection):
    pass


@servernet_job('hello', args={'from'})
def on_hello(connection, ctx):
    servernet_forward(on_request, connection)


@servernet_job('request-log')
def on_request_log(connection):
    pass


@servernet_job('request-log-from')
def on_request_log_from(connection):
    pass


@servernet_job('send-log')
def on_send_log(connection):
    pass


@servernet_job('reload')
def on_reload(connection):
    pass


@servernet_job('get-servers')
def on_get_servers(connection):
    pass


@servernet_job('get-banlist')
def on_get_banlist(connection):
    pass


@servernet_job('get-motd')
def on_get_motd(connection):
    pass


@servernet_job('get-motd-expires')
def on_get_motd_expires(connection):
    pass


@servernet_job('get-mirrors', sync=True)
def on_get_mirrors(connection):
    mirrors = db.read_mirrors()
    payload = connection.encode_payload([
        dict(name=name, address=address)
        for name, address in mirrors
    ])
    connection.write(payload)


servernet_job('ping', func=None, sync=False)
