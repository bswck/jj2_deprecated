from __future__ import annotations

import dataclasses
import datetime
import functools
import inspect
import json
import typing

import blinker
from loguru import logger

from jj2 import constants
from jj2 import endpoints
from jj2.listservers import db
from jj2.listservers import entities

if typing.TYPE_CHECKING:
    from typing import Any
    from typing import Callable
    from typing import Iterable


@dataclasses.dataclass
class Job:
    action: str
    data: Iterable
    origin: str

    def validate(self):
        if not (
            isinstance(self.action, str)
            and isinstance(self.data, typing.Iterable)
            and isinstance(self.origin, str)
        ):
            raise ValueError('invalid ServerNet job commission')


class ServerNet(endpoints.Server):
    default_port = constants.DEFAULT_LISTSERVER_PORT.SERVERNET


@ServerNet.handler
class ServerNetHandler(endpoints.EndpointHandler):
    endpoint: ServerNet

    jobs = blinker.Namespace()
    job_class = Job

    decode_payload = staticmethod(json.loads)
    encode_payload = staticmethod(json.dumps)

    READ_SIZE = 2048
    MAX_READ_ATTEMPTS = 11
    UNSYNCED_JOBS = {
        'hello', 'request',
        'delist', 'request-log',
        'send-log', 'request-log-from'
    }

    def __init__(
        self,
        endpoint: ServerNet,
        reader, writer,
        mirror_pool: endpoints.HandlerPool
    ):
        super().__init__(endpoint, reader, writer)
        self.buffer = bytearray()
        self.read_attempts = 0
        self.sync_chunks = set()
        self.mirror_pool = mirror_pool
        self.synced = None
        self.context = {}
        self.job = None

    # @endpoints.validation_backend(ServerNet)
    async def validate(self, pool=None):
        if self.endpoint.is_ssl:
            if not self.is_localhost:
                logger.warning(
                    f'Outside IP {self.host} '
                    'tried to connect to the remote admin API'
                )
                return self.stop()
        elif (
            self.host not in db.read_mirrors().values()
            or self.is_localhost
        ):
            logger.warning(
                'Unauthorized ServerNet connection '
                f'from {self.host}:{self.port}'
            )
            return self.stop()
        db.update_mirror(self.host)

    @property
    def can_sync(self):
        return (
            self.sync_chunks
            and self.job
            and self.job.action not in self.UNSYNCED_JOBS
        )

    async def attempt_read(self):
        try:
            data = await self.read(self.READ_SIZE)
        except OSError:
            logger.error(
                f'ServerNet connection from {self.host} '
                'broke while receiving data'
            )
            return self.stop()
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
            return self.stop()

        if not all(payload.get_next_datagram(key) for key in dataclasses.fields(Job)):
            logger.error(
                f'ServerNet update received from {self.host}, '
                'but JSON was incomplete'
            )
            return self.stop()

        if payload['origin'].strip() == self.local_address:
            return self.stop()

        job_object = self.job_class(**payload)
        try:
            job_object.validate()
        except ValueError:
            logger.error(
                f'ServerNet update received from {self.host}, '
                'but the data was incorrect'
            )
            return self.stop()

        self.job = job_object

    async def communicate(self, pool=None):
        await self.attempt_read()
        payload = self.attempt_decode()
        if self.read_attempts > self.MAX_READ_ATTEMPTS:
            return self.stop()
        if payload:
            self.make_job(payload)
        if self.job:
            try:
                self.dispatch_job()
            finally:
                self.stop()

    def dispatch_job(self):
        sender = type(self)
        action_name = self.job.action
        for subcontext in self.context:
            signal = self.jobs.signal(action_name)
            signal.sendto(sender, **subcontext)
        if self.can_sync:
            self.sync_job()

    def commission(self, action, *data, origin=None):
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


_ORIG_FUNC_ATTR = 'func'
_CONTEXT_ALIAS = 'ctx'
_FUNC_MISSING = object()


def job(
    action: str,
    func: Callable | object | None = _FUNC_MISSING, *,
    args: set[str] | None = None,
    defaults: dict[str, Any] | None = None,
    sync: bool | None = None
):
    """Register a ServerNet job callback."""

    def job_decorator(decorated_func: Callable):
        nonlocal func
        if not decorated_func:
            func = decorated_func
        parameters = inspect.signature(func).parameters
        variadic_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        @functools.wraps(func)
        def job_wrapper(connection: ServerNetHandler, /, **context: Any):
            compat_action = connection.job.action == action

            args_ok = True

            for argument in (args or set()):
                value = context.get(argument)
                if value is None:
                    missing = object()
                    value = defaults.get(argument, missing)
                    if value is missing:
                        logger.error(
                            'Received incomplete server data '
                            f'from ServerNet connection {connection.host}'
                        )
                        connection.synced = False
                        args_ok = False

            if args_ok:
                if not variadic_kwargs:
                    context = {
                        param: value
                        for param, value in context.items()
                        if param in parameters
                    }

                if _CONTEXT_ALIAS in parameters:
                    context[_CONTEXT_ALIAS] = connection.context

                if sync is not None:
                    connection.synced = sync

                func(connection, **context)

            do_sync = connection.synced

            if compat_action and do_sync:
                updated_ctx = connection.context
                connection.sync_chunks.add(updated_ctx)

        wrapper = (
            ServerNetHandler
            .jobs.signal(action)
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


def job_forward(func, *args, **kwargs):
    func = getattr(func, _ORIG_FUNC_ATTR, func)
    return func(*args, **kwargs)


@job('server', args={'id'}, sync=True)
def on_server(_, ctx):
    server_id = ctx['id']
    ctx.update(remote=1)
    db.update_server(server_id, **ctx)


@job('add-banlist', sync=True)
def on_add_banlist(connection: ServerNetHandler, ctx):
    ctx.setdefault(origin=connection.local_host)
    db.create_banlist_entry(**ctx)
    logger.info(
        f'Added banlist entry via ServerNet connection {connection.host}'
    )


@job('delete-banlist', sync=True)
def on_delete_banlist(connection: ServerNetHandler, ctx):
    ctx.setdefault(origin=connection.local_host)
    db.delete_banlist_entry(**ctx)
    logger.info(
        f'Removed banlist entry via ServerNet connection {connection.host}'
    )


@job('delist', args={'id'}, sync=True)
def on_delist(connection: ServerNetHandler, ctx):
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
def on_add_mirror(connection: ServerNetHandler, name: str, address: str):
    if db.read_mirror(name, address):
        logger.info(
            f'Mirror {connection.host} tried to add mirror {address}, '
            f'but name or address already known'
        )
        return
    if name == constants.LISTSERVER_RESERVED_MIRROR_NAME:
        logger.error(
            f'{constants.LISTSERVER_RESERVED_MIRROR_NAME} is a reserved '
            f'name for mirrors, %s tried to use it'
        )
        connection.synced = False
        return
    db.create_mirror(name, address)
    connection.commission('hello', {'from': connection.local_host})
    logger.info(f'Added mirror {address} via ServerNet connection {connection.host}')


@job('delete-mirror', args={'name', 'address'}, sync=True)
def on_delete_mirror(connection: ServerNetHandler, name: str, address: str):
    if not db.read_mirror(name, address):
        logger.info(
            f'Mirror {connection.host} tried to '
            f'remove mirror {address}, but not known'
        )
        return
    db.delete_mirror(name, address)
    logger.info(f'Deleted mirror {address} via ServerNet connection {connection.host}')


@job('send-motd', args={'motd', 'expires', 'motd-updated'}, sync=True)
def on_set_motd(connection: ServerNetHandler, ctx):
    motd, expires, updated = ctx['motd'], ctx['expires'], ctx['updated']
    current_motd = db.read_motd()
    if datetime.datetime.utcfromtimestamp(updated) < current_motd.updated:
        logger.info(f'Received MOTD update from {connection.host}, but own MOTD was more recent')
        connection.synced = False
        return
    db.update_motd(entities.MessageOfTheDay(motd, expires, updated))
    logger.info(f'Updated MOTD via ServerNet connection {connection.host}')


DEFAULT_SYNCED_FRAGMENTS = 'servers,banlist,mirrors,motd'
FRAGMENT_SYNCERS = {}


def fragment_syncer(name):
    return lambda fn: FRAGMENT_SYNCERS.update((name, fn)) or fn


def sync_fragment(name, /, *args, **kwargs):
    return FRAGMENT_SYNCERS[name](*args, **kwargs)


@fragment_syncer('servers')
def sync_servers(connection: ServerNetHandler):
    servers = db.read_servers()
    connection.commission('server', *(server.dict() for server in servers))


@fragment_syncer('banlist')
def sync_banlist(connection: ServerNetHandler):
    entries = db.read_banlist_entries()
    connection.commission('server', *(entry.dict() for entry in entries))


@fragment_syncer('mirrors')
def sync_mirrors(connection: ServerNetHandler):
    mirrors = db.read_mirrors()
    connection.commission('server', *(mirror.dict() for mirror in mirrors))


@fragment_syncer('mirrors')
def sync_motd(connection: ServerNetHandler):
    motd = db.read_motd()
    connection.commission('motd', motd.dict())


@job('request', args={'fragment'}, defaults={'fragment': None})
def on_request(connection: ServerNetHandler, ctx):
    fragments = (ctx['fragment'] or DEFAULT_SYNCED_FRAGMENTS).replace(',', ' ').split()
    for fragment in fragments:
        sync_fragment(fragment, connection)


@job('hello', args={'from'})
def on_hello(connection: ServerNetHandler, ctx):
    job_forward(on_request, connection)


@job('request-log')
def on_request_log(connection: ServerNetHandler):
    pass  # future


@job('request-log-from')
def on_request_log_from(connection: ServerNetHandler):
    pass  # future


@job('send-log')
def on_send_log(connection: ServerNetHandler):
    pass  # future


@job('reload')
def on_reload(connection: ServerNetHandler):
    pass  # future


@job('get-servers')
def on_get_servers(connection: ServerNetHandler):
    pass


@job('get-banlist')
def on_get_banlist(connection: ServerNetHandler):
    pass


@job('get-motd')
def on_get_motd(connection: ServerNetHandler):
    motd = db.read_motd()
    payload = connection.encode_payload(motd.dict())
    connection.write(payload)


@job('get-motd-expires')
def on_get_motd_expires(connection: ServerNetHandler):
    motd = db.read_motd()
    payload = connection.encode_payload(motd.dict())
    connection.write(payload)


@job('get-mirrors', sync=True)
def on_get_mirrors(connection: ServerNetHandler):
    mirrors = [mirror.dict() for mirror in db.read_mirrors()]
    payload = connection.encode_payload(mirrors)
    connection.write(payload)


job('ping', func=None, sync=False)
