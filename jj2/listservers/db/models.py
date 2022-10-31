import datetime

from sqlalchemy import text, event
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Text

from jj2.listservers.db.api import Base


class ServerModel(Base):
    __tablename__ = 'servers'

    id = Column(Text, primary_key=True)
    ip = Column(Text)
    port = Column(Integer)
    created = Column(Integer, server_default=text('0'))
    lifesign = Column(Integer, server_default=text('0'))
    last_ping = Column(Integer, server_default=text('0'))
    private = Column(Integer, server_default=text('0'))
    remote = Column(Integer, server_default=text('0'))
    origin = Column(Text)
    version = Column(Text, server_default='1.00')
    plusonly = Column(Integer, server_default=text('0'))
    mode = Column(Text, server_default='unknown')
    players = Column(Integer, server_default=text('0'))
    max = Column(Integer, server_default=text('0'))
    name = Column(Text)
    prefer = Column(Integer, server_default=text('0'))


class SettingModel(Base):
    __tablename__ = 'settings'

    item = Column(Text, primary_key=True)
    value = Column(Text)


@event.listens_for(SettingModel, 'after_create')
def setup_motd_fields(_t, connection, **_k):
    stmt = (
       f"INSERT INTO {SettingModel.__tablename__} "
       f"(item, value) VALUES (?, ?), (?, ?), (?, ?)"
    )
    values = (
        "motd", "",
        "motd-updated", "0",
        "motd-expires", (datetime.datetime.utcnow() + datetime.timedelta(3 * 86400)).timestamp()
    )
    connection.execute(stmt, values)


class BanlistEntryModel(Base):
    __tablename__ = 'banlist'
    address = Column(Text, primary_key=True)
    type = Column(Text, primary_key=True)
    note = Column(Text)
    origin = Column(Text)
    reserved = Column(Text, server_default='')


class MirrorModel(Base):
    __tablename__ = 'mirrors'
    name = Column(Text, primary_key=True)
    address = Column(Text, primary_key=True)
    lifesign = Column(Integer, server_default=text('0'))
