from loguru import logger

from jj2.endpoints import Connection, Server
from jj2.listservers import db


class MOTDConnection(Connection):
    MSG_ENCODING = 'ASCII'

    async def communicate(self, pool=None):
        logger.info(f"Sending MOTD to {self.host}")
        motd = db.read_motd()
        self.message(str(motd))
        self.kill()


class MOTDServer(Server):
    default_port = 10058
    connection_class = MOTDConnection
