from loguru import logger

from jj2 import endpoints
from jj2 import constants
from jj2.listservers import db
from jj2.listservers.entities import MessageOfTheDay


class MOTDServer(endpoints.TCPServer):
    default_port = constants.DEFAULT_LISTSERVER_PORT.MOTD


class MOTDClient(endpoints.TCPClient):
    default_host = constants.DEFAULT_LISTSERVER_HOST
    default_port = constants.DEFAULT_LISTSERVER_PORT.MOTD


@MOTDServer.handler
@MOTDClient.handler
class MOTDConnection(endpoints.ConnectionHandler):
    MSG_ENCODING = 'ASCII'

    def __post_init__(self, motd=None):
        self.motd = motd or MessageOfTheDay()

    @endpoints.communication_backend(MOTDServer)
    async def send_motd(self):
        logger.info(f'Sending MOTD to {self.host}')
        motd = db.read_motd()
        await self.message(str(motd))
        self.stop()

    @endpoints.communication_backend(MOTDClient)
    async def read_motd(self):
        logger.info(f'Reading MOTD from {self.host}')
        self.motd.text = (await self.read()).decode().strip()
        self.stop()


def get_motd(*addresses, client_class=MOTDClient, setup_timeout=0.7, timeout=1):
    client = client_class(handler_kwargs=dict(motd=(motd := MessageOfTheDay())))
    endpoints.start_race(client, *addresses, setup_timeout=setup_timeout, timeout=timeout)
    return motd


if __name__ == '__main__':
    cur_motd = get_motd(
        ['list.jj2.plus'],
        ['list.digiex.net'],
        ['list.pukenukem.com'],
    )
    print(cur_motd)
