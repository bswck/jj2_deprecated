"""UDP gateway of the JJ2 game server communication."""
from jj2 import endpoints


class GameDataClient(endpoints.UDPClient):
    MAGIC = b'00'

    def get_datagram_checksum(self, datagram):
        """Compute checksum of a game packet datagram."""

        buffer = self.MAGIC + datagram
        least = most = 1
        for char in buffer[2:]:
            least += char
            most += least
        return (
            (least % 251).to_bytes(1, 'little', signed=False)
            + (most % 251).to_bytes(1, 'little', signed=False)
        )

    async def validate_data(self, datagram, addr=None):
        checksum, datagram[:] = datagram[:2], datagram[2:]
        return checksum == self.get_datagram_checksum(datagram)


@GameDataClient.handler
class GameDataHandler(endpoints.DatagramEndpointHandler):
    async def handle_data(self, data, *args):
        """Handle a game packet."""
        pass
