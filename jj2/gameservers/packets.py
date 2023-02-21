"""Packet building and parsing routines with bitbin."""
import bitbin as bb
from bitbin import this


bb.set_endianness(bb.BIG_ENDIAN)


class GamePacket(bb.Struct):
    packet_id: bb.unsigned_char
    packet_data: bb.Switch[this.packet_id]


old_fmt = bb.models(GamePacket).packet_data


@old_fmt.register(0)
class PacketData(bb.Struct):
    length_prefix: bb.unsigned_char
    packet_id: bb.unsigned_char
    packet_data: bb.Switch[this.packet_id]


@old_fmt.register_default
class PacketData(bb.Struct):
    packet_id: bb.unsigned_char
    packet_data: bb.Switch[this.packet_id]
