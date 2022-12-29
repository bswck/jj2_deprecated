import functools
import pprint

from loguru import logger

from jj2 import endpoints
from jj2 import constants
from jj2.listservers import db
from jj2.listservers import entities


DEFAULT_PORT = 10057


class ASCIIListServer(endpoints.TCPServer):
    default_port = constants.DEFAULT_LISTSERVER_PORT.ASCIILIST


class ASCIIListClient(endpoints.TCPClient):
    default_host = constants.DEFAULT_LISTSERVER_HOST
    default_port = constants.DEFAULT_LISTSERVER_PORT.ASCIILIST


@ASCIIListServer.handler
@ASCIIListClient.handler
class ASCIIListConnection(endpoints.ConnectionHandler):
    MSG_ENCODING = 'ASCII'
    SPLITTER = '\r\n'

    def __post_init__(self, servers=None, memory_isolated=False):
        if servers is None:
            servers = []
        self.servers = servers
        self.memory_isolated = memory_isolated

    @endpoints.communication_backend(ASCIIListServer)
    async def send_list(self):
        logger.info(f'Sending ASCII server list to {self.str_address}')

        db.delete_remote_servers()
        servers = db.read_servers(
            bind_listserver=self.local_address,
            isolated=self.memory_isolated
        )
        self.message("".join(server.asciilist_repr for server in servers))
        self.stop()

    @endpoints.communication_backend(ASCIIListClient)
    async def read_list(self):
        logger.info(f'Reading ASCII server list from {self.str_address}')
        servers = (await self.read()).strip().decode().split(self.SPLITTER)
        loader = functools.partial(
            entities.GameServer.from_asciilist_repr,
            isolated=self.memory_isolated
        )
        self.servers.extend(filter(
            lambda server: server is not None and server not in self.servers,
            map(loader, servers))
        )
        self.stop()


def get_asciilist(*addresses, client_class=ASCIIListClient, setup_timeout=0.7, timeout=1):
    client = client_class(handler_kwargs=dict(servers=(servers := [])))
    endpoints.start_race(client, *addresses, setup_timeout=setup_timeout, timeout=timeout)
    return servers


if __name__ == '__main__':
    cur_list = get_asciilist(
        ['list.jj2.plus'],
        ['list.digiex.net'],
        ['list.pukenukem.com'],
    )
    pprint.pprint(cur_list)
