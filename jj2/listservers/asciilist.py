from loguru import logger

from jj2.networking.server import Connection, Server
from jj2.listservers import db


class ASCIIListConnection(Connection):
    msg_encoding = 'ASCII'

    async def serve(self):
        logger.info(f"Sending ASCII server list to {self.ip}")

        db.purge_remote_servers()
        servers = db.get_servers()
        self.msg("".join(str(server) for server in servers))
        self.kill()


class ASCIIListServer(Server):
    default_port = 10057
    connection_class = ASCIIListConnection
