from loguru import logger

from jj2.aiolib import AsyncConnection, AsyncServer
from jj2.listservers import db


class MOTDConnection(AsyncConnection):
    MSG_ENCODING = 'ASCII'

    async def run(self):
        logger.info(f"Sending MOTD to {self.ip}")
        motd = db.get_motd()
        self.msg(str(motd))
        self.kill()


class MOTDServer(AsyncServer):
    default_port = 10058
    connection_class = MOTDConnection
