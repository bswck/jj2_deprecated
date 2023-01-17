from constance.config import register_encoding, set_global_endianness, Endianness
from jj2.constants import CHAT_ENCODING

register_encoding(CHAT_ENCODING)
set_global_endianness(Endianness.LITTLE)

from .packets import *
