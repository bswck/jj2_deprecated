"""Packet building and parsing routines with constance."""
import collections

from constance import *


class PlusVersion(
    Constance,
    collections.namedtuple('PlusVersion', 'major minor')
):
    tp = Int32sl

    @classmethod
    def construct(cls):
        return cls.tp.construct()

    @classmethod
    def _load(cls, buf, _c):
        if isinstance(buf, int):
            return cls(minor=buf & 255, major=buf >> 16)
        return cls(*buf)

    def _data(self):
        major, minor = self
        return major << 16 | minor


class GamePacket(Struct):
    _skip_fields = ['plus']
    plus_version: PlusVersion = (5, 9)


if __name__ == '__main__':
    print(GamePacket([1, 0]))
