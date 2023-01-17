import collections

from constance import *

__all__ = (
    'PlusVersion',
)


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
            return cls(
                minor=buf & 0xff,
                major=buf >> 0x10
            )
        return cls(*buf)

    def _data(self):
        major, minor = self
        return major << 0x10 | minor
