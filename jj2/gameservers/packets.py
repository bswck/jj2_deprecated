"""Packet building and parsing routines with constance."""
from constance import *

from jj2.gameservers.entities import *


class GamePacket(Struct):
    _skip_fields = ['plus_version']
    plus_version: PlusVersion = (5, 9)


if __name__ == '__main__':
    print(GamePacket([1, 0]))
