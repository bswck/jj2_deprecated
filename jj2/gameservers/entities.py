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
    def _initialize(cls, buf, context):
        return cls(*buf)

    @classmethod
    def _load(cls, data, context=None, **kwargs):
        return cls(
            data & 0xff,
            data >> 0x10,
        )

    def _data(self):
        major, minor = self
        return major << 0x10 | minor
