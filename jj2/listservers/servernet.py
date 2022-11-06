import dataclasses
import functools
import inspect
import json
import typing

import blinker
from loguru import logger

from jj2.constants import LISTSERVER_RESERVED_MIRROR_NAME
from jj2.endpoints import Connection, Server
from jj2.listservers import db

if typing.TYPE_CHECKING:
    from typing import Any
    from typing import Callable
    from typing import Iterable
    from jj2.endpoints import ConnectionPool


@dataclasses.dataclass
class Job:
    action: 'str'
    data: 'Iterable'
    origin: 'str'

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
    UNSYNCED_JOBS = {
        'hello', 'request',
        'delist', 'request-log',
        'send-log', 'request-log-from'
    }

    def __init__(
        self,
        template: 'ServerNet',
        reader, writer,
        mirror_pool: 'ConnectionPool'
    ):
        super().__init__(template, reader, writer)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.sync_chunks = set()
        self.mirror_pool = mirror_pool
        self.synced = None
        self.ctx = {}
        self.job = None

    async def validate(self, pool=None):
        if self.template.is_ssl:
            if not self.is_localhost:
                logger.warning(
                    f'Outside IP {self.host} '
                    'tried to connect to the remote admin API'
                )
                self.kill()
        elif (
            self.host not in db.read_mirrors().values()
            or self.is_localhost
        ):
            logger.warning(
                'Unauthorized ServerNet connection '
                f'from {self.host}:{self.port}'
            )
            self.kill()
        db.update_mirror(self.host)

    @property
    def can_sync(self):
        return (
            self.sync_chunks
            and self.job
            and self.job.action not in self.UNSYNCED_JOBS
        )

    def attempt_read(self):
        try:
            data = self.read(2048)
        except OSError:
            logger.error(
                f'ServerNet connection from {self.host} '
                'broke while receiving data'
            )
            return self.kill()
        else:
            self.buffer.extend(data)
        self.read_attempts += 1

    def attempt_decode(self):
        payload = None
        if not self.job:
            try:
                data = self.buffer.decode(self.MSG_ENCODING, 'ignore')
                payload = self.decode_payload(data)
            except ValueError:
                pass
        return payload

    def make_job(self, payload):
        if self.buffer and not payload:
            logger.error(
                f'ServerNet update received from {self.host}, '
                f'but could not acquire valid payload (got {self.buffer})'
            )
            return self.kill()

        if not all(payload.get(key) for key in dataclasses.fields(Job)):
            logger.error(
                f'ServerNet update received from {self.host}, '
                'but JSON was incomplete'
            )
            return self.kill()

        if payload['origin'].strip() == self.local_address:
            return self.kill()

        job_obj = self.job_class(**payload)
        try:
            job_obj.validate()
        except ValueError:
            logger.error(
                f'ServerNet update received from {self.host}, '
                'but the data was incorrect'
            )
            return self.kill()

        self.job = job_obj

    async def communicate(self, pool=None):
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
        for sub_ctx in self.ctx:
            signal = self.job_ns.signal(action_name)
            signal.send(sender, **sub_ctx)
        if self.can_sync:
            self.sync_job()

    def request_job(self, action, *data, origin=None):
        if origin is None:
            origin = self.local_address
        job_obj = self.job_class(
            action=action,
            data=list(data),
            origin=origin
        )
        payload = self.encode_payload(job_obj)
        self.write(payload)

    def sync_job(self):
        job_obj = self.job_class(
            action=self.job.action,
            data=list(self.sync_chunks),
            origin=self.host
        )
        payload = self.encode_payload(job_obj)
        self.sync(self.mirror_pool, payload)


class ServerNet(Server):
    default_port = 10058
    connection_class = ServerNetConnection


_ORIG_FUNC_ATTR = 'func'
_CONTEXT_REFERENCE = 'ctx'
_FUNC_MISSING = object()


def job(
    action: 'str',
    func: 'Callable | object | None' = _FUNC_MISSING, *,
    args: 'set[str] | None' = None,
    sync: 'bool | None' = None
):
    """Register a ServerNet job callback."""

    def job_decorator(decorated_func: 'Callable'):
        nonlocal func
        if not decorated_func:
            func = decorated_func
        parameters = inspect.signature(func).parameters
        has_variadic_kw = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        @functools.wraps(func)
        def job_wrapper(connection: 'ServerNetConnection', /, **ctx: 'Any'):
            compat_action = connection.job.action == action

            args_ok = True

            for arg in (args or set()):
                value = ctx.get(arg)
                if value is None:
                    logger.error(
                        'Received incomplete server data '
                        f'from ServerNet connection {connection.host}'
                    )
                    connection.synced = False
                    args_ok = False

            if args_ok:
                if not has_variadic_kw:
                    ctx = {
                        param: value
                        for param, value in ctx.items()
                        if param in parameters
                    }

                if _CONTEXT_REFERENCE in parameters:
                    ctx[_CONTEXT_REFERENCE] = connection.ctx

                if sync is not None:
                    connection.synced = sync

                func(connection, **ctx)

            do_sync = connection.synced

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


@job('server', args={'id'}, sync=True)
def on_server(_, ctx):
    server_id = ctx['id']
    ctx.update(remote=1)
    db.update_server(server_id, **ctx)


@job('add-banlist', sync=True)
def on_add_banlist(connection, ctx):
    ctx.setdefault(origin=connection.create_endpoint.host)
    db.create_banlist_entry(**ctx)
    logger.info(
        f'Added banlist entry via ServerNet connection {connection.host}'
    )


@job('delete-banlist', sync=True)
def on_delete_banlist(connection, ctx):
    ctx.setdefault(origin=connection.create_endpoint.host)
    db.delete_banlist_entry(**ctx)
    logger.info(
        f'Removed banlist entry via ServerNet connection {connection.host}'
    )


@job('delist', args={'id'}, sync=True)
def on_delist(connection, ctx):
    server_id = ctx['id']
    server = db.read_server(server_id)
    if server:
        if server.remote:
            db.delete_server(server_id)
            logger.info(f'Delisted server via ServerNet connection {connection.host}')
        else:
            logger.error(
                f'Mirror {connection.host} tried '
                f'to delist server {server_id}, '
                'but the server was not remote!'
            )
            connection.synced = False
    else:
        logger.error(
            f'Mirror {connection.host} tried to '
            f'delist server {server_id}, but the server was unknown'
        )
        connection.synced = False


@job('add-mirror', args={'name', 'address'}, sync=True)
def on_add_mirror(connection, name, address):
    if db.read_mirror(name, address):
        logger.info(
            f'Mirror {connection.host} tried to add mirror {address}, '
            f'but name or address already known'
        )
        return
    if name == LISTSERVER_RESERVED_MIRROR_NAME:
        logger.error(
            f'{LISTSERVER_RESERVED_MIRROR_NAME} is a reserved '
            f'name for mirrors, %s tried to use it'
        )
        connection.synced = False
        return
    db.create_mirror(name, address)
    connection.request_job('hello', {'from': connection.local_host})
    logger.info(f'Added mirror {address} via ServerNet connection {connection.host}')


@job('delete-mirror', args={'name', 'address'}, sync=True)
def on_delete_mirror(connection, name, address):
    if not db.read_mirror(name, address):
        logger.info(
            f'Mirror {connection.host} tried to '
            f'remove mirror {address}, but not known'
        )
        return
    db.delete_mirror(name, address)
    logger.info(f'Deleted mirror {address} via ServerNet connection {connection.host}')


@job('send-motd')
def on_set_motd(connection):
    pass


@job('request')
def on_request(connection):
    pass


@job('hello', args={'from'})
def on_hello(connection, ctx):
    servernet_forward(on_request, connection)


@job('request-log')
def on_request_log(connection):
    pass


@job('request-log-from')
def on_request_log_from(connection):
    pass


@job('send-log')
def on_send_log(connection):
    pass


@job('reload')
def on_reload(connection):
    pass


@job('get-servers')
def on_get_servers(connection):
    pass


@job('get-banlist')
def on_get_banlist(connection):
    pass


@job('get-motd')
def on_get_motd(connection):
    pass


@job('get-motd-expires')
def on_get_motd_expires(connection):
    pass


@job('get-mirrors', sync=True)
def on_get_mirrors(connection):
    mirrors = [dataclasses.asdict(mirror) for mirror in db.read_mirrors()]
    payload = connection.encode_payload(mirrors)
    connection.write(payload)


job('ping', func=None, sync=False)
