from loguru import logger

from jj2.aiolib import AsyncConnection, AsyncServer
from jj2.listservers import db


class ASCIIListConnection(AsyncConnection):
    MSG_ENCODING = 'ASCII'

    async def run(self):
        logger.info(f"Sending ASCII server list to {self.ip}")

        db.purge_remote_servers()
        servers = db.get_servers()
        self.msg("".join(str(server) for server in servers))
        self.kill()


class ASCIIListServer(AsyncServer):
    default_port = 10057
    connection_class = ASCIIListConnection
