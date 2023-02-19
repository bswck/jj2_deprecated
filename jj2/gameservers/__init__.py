from bitbin.config import register_encoding, set_endianness, Endianness
from jj2.constants import CHAT_ENCODING

register_encoding(CHAT_ENCODING)
set_endianness(Endianness.LITTLE)

from .packets import *
