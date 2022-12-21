from __future__ import annotations

from loguru import logger

from jj2.endpoints import Connection, Server
from jj2.listservers import db, entities


DUMMY_SERVERS = (
    entities.GameServer(address='192.0.2.0', port=80, name='Get JJ2 Plus, a mod for JJ2!'),
    entities.GameServer(address='192.0.2.1', port=80, name='Download at get.jj2.plus'),
    entities.GameServer(address='192.0.2.2', port=80, name='Check out jj2multiplayer.web.app'),
    entities.GameServer(address='192.0.2.3', port=80, name='--------------------------------'),
)


class BinaryListConnection(Connection):
    payload_header: bytes = b"\x07LIST\x01\x01"
    architecture: BinaryListServer

    async def communicate(self, pool=None):
        logger.info(f'Sending binary server list to {self.host}')

        db.delete_remote_servers()
        servers = []
        if self.architecture.use_dummy_servers:
            servers.extend(DUMMY_SERVERS)
        servers.extend(db.read_servers(vanilla=True, bind_listserver=self.local_address))
        payload = self.payload_header + b''.join(server.binarylist_repr for server in servers)
        self.send(payload)
        self.kill()


class BinaryListServer(Server):
    default_port = 10053
    connection_class = BinaryListConnection
    use_dummy_servers = True
