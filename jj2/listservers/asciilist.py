from loguru import logger

from jj2.endpoints import Connection, Server
from jj2.listservers import db


class ASCIIListConnection(Connection):
    MSG_ENCODING = 'ASCII'

    async def _cycle(self):
        logger.info(f"Sending ASCII server list to {self.address}")

        db.delete_remote_servers()
        servers = db.read_servers(bind_listserver=self.endpoint.job)
        self.message("".join(server.asciilist_repr for server in servers))
        self.kill()


class ASCIIListServer(Server):
    default_port = 10057
    connection_class = ASCIIListConnection
