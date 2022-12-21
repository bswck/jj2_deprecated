import datetime

from jj2.listservers.entities import BanlistEntry
from jj2.listservers.entities import GameServer
from jj2.listservers.entities import MessageOfTheDay
from jj2.listservers.entities import Mirror
from jj2.listservers.db.api import get_session
from jj2.listservers.db.models import BanlistEntryModel
from jj2.listservers.db.models import MirrorModel
from jj2.listservers.db.models import ServerModel
from jj2.listservers.db.models import SettingModel


__all__ = (
    'read_server',
    'read_servers',
    'update_server',
    'delete_remote_servers',
    'delete_server',

    'create_mirror',
    'read_mirror',
    'read_mirrors',
    'update_mirror',
    'delete_mirror',

    'create_banlist_entry',
    'read_banlist_entries',
    'read_banlist_entry',
    'delete_banlist_entry',

    'read_motd',
)


# Servers

def read_servers(
    vanilla: bool = False,
    mirror: bool = False,
    bind_listserver: str | None = None,
    server_cls: type[GameServer] | None = GameServer,
):
    with get_session() as session:
        query = session.query(ServerModel)
        if not mirror:
            query = query.filter(ServerModel.max > 0)
        if vanilla:
            query = query.filter(ServerModel.plusonly == 0)
        query = query.order_by(
            ServerModel.prefer.desc(),
            ServerModel.private.asc(),
            (ServerModel.players == ServerModel.max).asc(),
            ServerModel.players.desc(),
            ServerModel.created.asc()
        )
        server_models = query.all()
    return [
        server_from_model(
            server_model,
            server_cls=server_cls,
            bind_listserver=bind_listserver
        )
        if server_cls else server_model
        for server_model in server_models
    ]


def update_server(server_id: str, **traits):
    with get_session() as session:
        server = read_server(server_id)
        if not server:
            server = ServerModel(id=server_id)
        for column, value in traits.items():
            setattr(server, column, value)
        session.add(server)
        session.commit()


def server_from_model(
    server_model: ServerModel,
    server_cls: type[GameServer] | None = GameServer,
    bind_listserver: str | None = None
):
    server = None
    if server_model:
        server = server_cls(
            address=server_model.ip,
            port=server_model.port,
            remote=bool(server_model.remote),
            private=bool(server_model.private),
            mode=server_model.mode,
            version=server_model.version,
            listed_at=datetime.datetime.utcfromtimestamp(server_model.created),
            clients=server_model.players,
            max_clients=server_model.max,
            name=server_model.name,
        )
        if bind_listserver:
            server.listserver = bind_listserver
    return server


def read_server(
    server_id: str,
    server_cls: type[GameServer] | None = GameServer,
    bind_serverlist: str | None = None
):
    with get_session() as session:
        server_model = session.get(ServerModel, server_id)
        if server_cls:
            return server_from_model(
                server_model,
                server_cls=server_cls,
                bind_listserver=bind_serverlist
            )
        return server_model


def delete_remote_servers(timeout: int | None = 40):
    with get_session() as session:
        query = (
            session
            .query(ServerModel)
            .filter(ServerModel.remote == 1)
        )
        if timeout:
            query.filter(
                ServerModel.lifesign < (
                    datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout)
                ).timestamp()
            )
        query.delete()
        session.commit()


def delete_server(server_id: str):
    server_model = read_server(server_id, server_cls=None)
    with get_session() as session:
        session.delete(server_model)
        session.commit()


# Mirrors

def create_mirror(name: str, address: str):
    with get_session() as session:
        mirror_model = MirrorModel(name, address)
        session.add(mirror_model)
        session.commit()


def read_mirror(name: str, address: str, mirror_cls: type[Mirror] | None = Mirror):
    with get_session() as session:
        mirror_model = session.get(MirrorModel, [name, address])
    ret = None
    if mirror_model:
        if mirror_cls:
            ret = Mirror(mirror_model.name, mirror_model.host)
        else:
            ret = mirror_model
    return ret


def read_mirrors(mirror_cls: type[Mirror] | None = Mirror):
    with get_session() as session:
        mirror_models = session.query(MirrorModel).all()
    return [
        Mirror(mirror_model.name, mirror_model.address)
        if mirror_cls else mirror_model
        for mirror_model in mirror_models
    ]


def update_mirror(address: str):
    with get_session() as session:
        (
            session
            .query(MirrorModel)
            .filter_by(address=address)
            .update(dict(lifesign=datetime.datetime.utcnow()))
        )
        session.commit()


def delete_mirror(name: str, address: str):
    with get_session() as session:
        mirror_model = read_mirror(name, address, mirror_cls=None)
        if mirror_model:
            session.delete(mirror_model)
            session.commit()


# Banlist entries

def create_banlist_entry(**model_args):
    existing_entry_model = read_banlist_entry(**model_args, entry_cls=None)
    if existing_entry_model:
        delete_banlist_entry(entry_model=existing_entry_model)
    else:
        with get_session() as session:
            entry_model = BanlistEntryModel(**model_args)
            session.add(entry_model)
            session.commit()


def read_banlist_entries(
    *, entry_cls: type[BanlistEntry] | None = BanlistEntry,
    **criterion
):
    with get_session() as session:
        return [
            BanlistEntry(**entry_model._mapping)
            if entry_cls else entry_model
            for entry_model in (
                session
                .query(BanlistEntryModel)
                .filter_by(**criterion)
                .all()
            )
        ]


def read_banlist_entry(
    *, entry_cls: type[BanlistEntry] | None = BanlistEntry,
    **criterion
) -> 'BanlistEntry | BanlistEntryModel | None':
    entries = read_banlist_entries(entry_cls=entry_cls, **criterion)
    return entries[0] if entries else None


def delete_banlist_entry(entry_model: BanlistEntryModel | None = None, **model_args):
    if entry_model is None:
        entry_model = read_banlist_entry(**model_args, entry_cls=None)
    with get_session() as session:
        session.delete(entry_model)
        session.commit()


# MOTD

def read_motd(motd_cls: type[MessageOfTheDay] | None = MessageOfTheDay):
    expires_model = None
    with get_session() as session:
        model = session.get(SettingModel, "motd")
        if model:
            text = model.value
            expires_model = session.get(SettingModel, "motd-expires")
    expires = None
    if expires_model:
        expires = expires_model.value
    if motd_cls:
        return MessageOfTheDay(text, expires)
    return model, expires_model
