import bitbin as bb

__all__ = (
    'PlusVersion',
)


class PlusVersion(bb.Struct):
    major: bb.unsigned_short
    minor: bb.unsigned_short
