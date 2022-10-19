from loguru import logger

from jj2.classes import GameServer
from jj2.networking.server import Connection, Server
from jj2.listservers import db


DUMMY_SERVERS = [
    GameServer(ip='192.0.2.0', port=80, name='Get JJ2 Plus, a mod for JJ2!'),
    GameServer(ip='192.0.2.1', port=80, name='Download at get.jj2.plus'),
    GameServer(ip='192.0.2.2', port=80, name='Check out jj2multiplayer.web.app'),
    GameServer(ip='192.0.2.3', port=80, name='--------------------------------'),
]


class BinaryListConnection(Connection):
    magic_header = b"\x07LIST\x01\x01"

    async def serve(self):
        logger.info(f"Sending binary server list to {self.ip}")

        db.purge_remote_servers()
        servers = []
        if self.server.use_dummy_servers:
            servers.extend(DUMMY_SERVERS)
        servers.extend(db.get_servers(vanilla=True))
        self.send(
            self.magic_header
            + b"".join(map(bytes, servers))
        )
        self.kill()


class BinaryListServer(Server):
    default_port = 10053
    connection_class = BinaryListConnection
    use_dummy_servers = True
