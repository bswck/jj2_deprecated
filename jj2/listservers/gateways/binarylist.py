from __future__ import annotations

import pprint

from loguru import logger

from jj2 import endpoints
from jj2 import constants
from jj2.listservers import db
from jj2.listservers import entities

DUMMY_SERVERS = (
    entities.GameServer(ip_address='192.0.2.0', port=80, name='Get JJ2 Plus, a mod for JJ2!'),
    # I prefer 'get' to 'www', sorry
    # https://github.com/stijnstijn/j2lsnek/commit/9135f85cc6299f35f2cbb8776c8a6efabec98732
    entities.GameServer(ip_address='192.0.2.1', port=80, name='Download at get.jj2.plus'),
    entities.GameServer(ip_address='192.0.2.3', port=80, name='--------------------------------'),
)


class BinaryListServer(endpoints.TCPServer):
    default_port = constants.DEFAULT_LISTSERVER_PORT.BINARYLIST
    use_dummy_servers = True


class BinaryListClient(endpoints.TCPClient):
    default_host = constants.DEFAULT_LISTSERVER_HOST
    default_port = constants.DEFAULT_LISTSERVER_PORT.BINARYLIST


@BinaryListServer.handler
@BinaryListClient.handler
class BinaryListConnection(endpoints.ConnectionHandler):
    payload_header: bytes = b'\x07LIST\x01\x01'
    endpoint_class: BinaryListServer

    def __post_init__(self, servers=None, memory_isolated=False):
        if servers is None:
            servers = []
        self.servers = servers
        self.memory_isolated = memory_isolated

    @endpoints.communication_backend(BinaryListServer)
    async def send_list(self):
        logger.info(f'Sending binary server list to {self.str_address}')

        db.delete_remote_servers()
        servers = []
        if self.endpoint_class.use_dummy_servers:
            servers.extend(DUMMY_SERVERS)
        servers.extend(db.read_servers(vanilla=True, bind_listserver=self.local_address))
        payload = self.payload_header + b''.join(server.binarylist_repr for server in servers)
        self.send(payload)
        self.stop()

    @endpoints.communication_backend(BinaryListClient)
    async def read_list(self):
        logger.info(f'Reading binary server list from {self.str_address}')
        servers = (await self.read()).strip()
        if not servers.startswith(self.payload_header):
            logger.info(f'Invalid binary server list payload from {self.str_address}')
            return self.stop()
        servers = servers.removeprefix(self.payload_header)
        loaded = entities.GameServer.from_binarylist_reprs(servers, isolated=self.memory_isolated)
        self.servers.extend(filter(
            lambda server: server is not None and server not in self.servers, loaded
        ))
        self.stop()


def get_binarylist(*addresses, client_class=BinaryListClient, setup_timeout=0.4, timeout=1):
    client = client_class(handler_kwargs=dict(servers=(servers := [])))
    endpoints.start_race(client, *addresses, setup_timeout=setup_timeout, timeout=timeout)
    return servers


if __name__ == '__main__':
    cur_list = get_binarylist(
        ['list.jj2.plus'],
        ['list.digiex.net'],
        ['list.pukenukem.com'],
    )
    pprint.pprint(cur_list)
