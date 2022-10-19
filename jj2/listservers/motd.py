from loguru import logger

from jj2.networking.server import Connection, Server
from jj2.listservers import db


class MOTDConnection(Connection):
    msg_encoding = 'ASCII'

    async def serve(self):
        logger.info(f"Sending MOTD to {self.ip}")
        motd = db.get_motd()
        self.msg(str(motd))
        self.kill()


class MOTDServer(Server):
    default_port = 10058
    connection_class = MOTDConnection
