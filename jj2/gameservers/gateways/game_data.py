"""UDP gateway of the JJ2 game server communication."""
import construct as cs

from jj2 import endpoints


class GameDataClient(endpoints.UDPClient):
    MAGIC = b'00'

    def datagram_checksum(self, datagram):
        """Compute checksum of a game packet datagram."""

        buffer = self.MAGIC + datagram
        least = most = 1
        for char in buffer[2:]:
            least += char
            most += least
        return cs.Byte.build(least % 251) + cs.Byte.build(most % 251)

    def validate_data(self, datagram, addr=None):
        checksum = bytes((datagram.pop(0), datagram.pop(0)))
        return checksum == self.datagram_checksum(datagram)


@GameDataClient.handler
class GameDataHandler(endpoints.DatagramEndpointHandler):
    async def communicate(self, pool=None):
        await self.queue_looping()




