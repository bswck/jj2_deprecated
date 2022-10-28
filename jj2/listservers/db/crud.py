import datetime

from jj2.classes import GameServer
from jj2.classes import MessageOfTheDay
from jj2.classes import BanlistEntry
from jj2.listservers.db.setup import get_session

from jj2.listservers.db.models import BanlistEntryModel
from jj2.listservers.db.models import ServerModel
from jj2.listservers.db.models import SettingModel
from jj2.listservers.db.models import MirrorModel


__all__ = (
    'get_server',
    'get_servers',
    'update_server',
    'delete_server',
    'add_banlist_entry',
    'get_banlist_entry',
    'get_banlist_entries',
    'delete_banlist_entry',
    'purge_remote_servers',
    'get_motd',
    'get_mirrors',
    'update_lifesign',
)


def get_motd():
    with get_session() as session:
        motd_text = session.get(SettingModel, "motd")
        soon = (datetime.datetime.utcnow() + datetime.timedelta(seconds=10)).timestamp()
        expires = session.get(SettingModel, "motd-expires") or soon
    return MessageOfTheDay(motd_text, expires)


def get_servers(
    vanilla: bool = False,
    mirror: bool = False,
    bind_serverlist: str | None = None,
    _cast: bool = True
):
    with get_session() as session:
        query = session.query(ServerModel)
        if not mirror:
            query = query.filter(ServerModel.max > 0)
        if vanilla:
            query = query.filter(ServerModel.plusonly == 0)
        models = query.order_by(
            ServerModel.prefer.desc(),
            ServerModel.private.asc(),
            (ServerModel.players == ServerModel.max).asc(),
            ServerModel.players.desc(),
            ServerModel.created.asc()
        ).all()
    return [
        GameServer.from_orm(model, serverlist=bind_serverlist)
        if _cast else model
        for model in models
    ]


def update_server(server_id, **traits):
    with get_session() as session:
        server = get_server(server_id)
        if not server:
            server = ServerModel(id=server_id)
        for column, value in traits.items():
            setattr(server, column, value)
        session.add(server)
        session.commit()


def get_server(server_id, _cast=True):
    with get_session() as session:
        server_model = session.get(ServerModel, server_id)
        if _cast:
            return GameServer.from_orm(server_model)
        return server_model


def delete_server(server_id):
    server_model = get_server(server_id, _cast=False)
    with get_session() as session:
        session.delete(server_model)
        session.commit()


def get_banlist_entries(_cast=True, **filter_by_args):
    with get_session() as session:
        return [
            BanlistEntry(**entry_model._mapping)
            if _cast else entry_model
            for entry_model in session.query(
                BanlistEntryModel
            ).filter_by(**filter_by_args).all()
        ]


def get_banlist_entry(**filter_by_args):
    entries = get_banlist_entries(**filter_by_args)
    return entries[0] if entries else None


def add_banlist_entry(**model_args):
    existing_entry_model = get_banlist_entry(**model_args, _cast=False)
    if existing_entry_model:
        delete_banlist_entry(entry_model=existing_entry_model)
    else:
        with get_session() as session:
            entry_model = BanlistEntryModel(**model_args)
            session.add(entry_model)
            session.commit()


def delete_banlist_entry(entry_model=None, **model_args):
    if entry_model is None:
        entry_model = get_banlist_entry(**model_args, _cast=False)
    with get_session() as session:
        session.delete(entry_model)
        session.commit()


def get_mirrors():
    with get_session() as session:
        mirrors = session.query(MirrorModel).all()
    return {mirror.name: mirror.address for mirror in mirrors}


def purge_remote_servers(timeout=40):
    with get_session() as session:
        session.query(ServerModel).filter(
            ServerModel.remote == 1,
            ServerModel.lifesign < (
                datetime.datetime.utcnow()
                - datetime.timedelta(seconds=timeout)
            ).timestamp()
        ).delete()
        session.commit()


def update_lifesign(address):
    with get_session() as session:
        session.query(MirrorModel).filter_by(
            address=address
        ).update(dict(lifesign=datetime.datetime.utcnow()))
        session.commit()
