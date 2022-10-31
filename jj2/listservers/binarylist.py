from loguru import logger

from jj2.endpoints import Connection, Server
from jj2.listservers import db
from jj2.listservers.classes import GameServer


DUMMY_SERVERS = (
    GameServer(address='192.0.2.0', port=80, name='Get JJ2 Plus, a mod for JJ2!'),
    GameServer(address='192.0.2.1', port=80, name='Download at get.jj2.plus'),
    GameServer(address='192.0.2.2', port=80, name='Check out jj2multiplayer.web.app'),
    GameServer(address='192.0.2.3', port=80, name='--------------------------------'),
)


class BinaryListConnection(Connection):
    payload_header = b"\x07LIST\x01\x01"

    async def run_once(self, pool=None):
        logger.info(f"Sending binary server list to {self.address}")

        db.delete_remote_servers()
        servers = []
        if self.endpoint.use_dummy_servers:
            servers.extend(DUMMY_SERVERS)
        servers.extend(db.read_servers(vanilla=True, bind_listserver=self.endpoint.address))
        payload = self.payload_header + b"".join(server.binarylist_repr for server in servers)
        self.send(payload)
        self.kill()


class BinaryListServer(Server):
    default_port = 10053
    connection_class = BinaryListConnection
    use_dummy_servers = True
