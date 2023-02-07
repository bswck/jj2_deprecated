"""Packet building and parsing routines with constance."""
from constance import *

from jj2.gameservers.entities import *

#
# class GamePacket(Struct):
#     _skip_fields = ['plus_version']
#     plus_version: PlusVersion = (5, 9)


# class GamePacketHeader(Struct):
#     _skip_fields = ['plus_version']
#     plus_version: PlusVersion[2] = ((5, 9), (5, 3))


print(type(PlusVersion[2]))