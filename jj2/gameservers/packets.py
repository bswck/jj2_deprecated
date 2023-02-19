"""Packet building and parsing routines with bitbin."""
import bitbin as bb

from jj2.gameservers.entities import *


class GamePacket(bb.Struct):
    packet_id: bb.Switch[int]
    data: bytes




