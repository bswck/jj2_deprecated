import datetime

from jj2.classes import GameServer, MessageOfTheDay
from jj2.listservers.db.base import get_session

from jj2.listservers.db.models import ServerModel
from jj2.listservers.db.models import SettingModel
from jj2.listservers.db.models import MirrorModel


__all__ = (
    'get_server',
    'update_server',
    'get_servers',
    'get_motd',
    'get_mirrors',
    'purge_remote_servers',
    'update_lifesign',
)


def get_motd():
    with get_session() as session:
        motd_text = session.get(SettingModel, "motd")
        expires = (
            session.get(SettingModel, "motd-expires")
            or (datetime.datetime.utcnow() + datetime.timedelta(seconds=10)).timestamp()
        )
    return MessageOfTheDay(motd_text, expires)


def get_servers(vanilla: bool = False):
    with get_session() as session:
        query = session.query(ServerModel).filter(ServerModel.max > 0)
        if vanilla:
            query = query.filter(ServerModel.plusonly == 0)
        models = query.order_by(
            ServerModel.prefer.desc(),
            ServerModel.private.asc(),
            (ServerModel.players == ServerModel.max).asc(),
            ServerModel.players.desc(),
            ServerModel.created.asc()
        ).all()
    return [GameServer.from_orm(model) for model in models]


def update_server(server_id, **traits):
    with get_session() as session:
        server = get_server(server_id)
        if not server:
            server = ServerModel(id=server_id)
        for column, value in traits.items():
            setattr(server, column, value)
        session.add(server)
        session.commit()


def get_server(server_id):
    with get_session() as session:
        return GameServer.from_orm(
            session.get(ServerModel, server_id)
        )


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
