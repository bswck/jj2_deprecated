"""
Introducing packets:
data-holding classes that can be serialized into bytes and then recreated from them.

Unlike pickling, safety of this process is guaranteed to be safe and is fully
designed by the programmer.

Works with the _construct_ library behind the scenes.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import inspect
import types
import typing
import weakref

import construct as cs

from jj2.constants import DEFAULT_COMMUNICATION_ENCODING


class ConstructFactory:
    @classmethod
    def construct(cls) -> cs.Construct:
        """Return a construct for self-serialization and self-deserialization."""
        raise NotImplementedError


_PACKET_FIELDS = '__packet_fields__'


def field(construct_factory, **kwargs) -> dataclasses.Field:
    metadata = kwargs.setdefault('metadata', {})
    metadata.update(metadata={'construct_factory': construct_factory})
    return dataclasses.field(**kwargs)


def field_construct(packet_field, fallback_type_hint, cls_name=None):
    name = packet_field.name
    qualname = (cls_name + '.' if cls_name else '') + (name or '')
    packet_field.metadata = dict(packet_field.metadata)
    construct_factory = (
        packet_field.metadata.get('construct_factory')
        or packet_field.metadata.setdefault(
            'construct_factory',
            factory_from_type_hint(fallback_type_hint, qualname)
        )
    )
    construct = cs.extractfield(_call_field_construct(construct_factory))
    if not construct:
        raise ValueError
    if name:
        return cs.Renamed(construct, name)
    return construct


@cs.singleton
def _monkeypatch_construct_encodings():
    cs.possiblestringencodings['cp1250'] = 1


class _TypingLib:
    _BUILTIN_TYPES_COUNTERPARTS = {}

    @classmethod
    def make_builtin_types(cls):
        if not cls._BUILTIN_TYPES_COUNTERPARTS:
            cls._BUILTIN_TYPES_COUNTERPARTS.update(
                filter(None, map(cls.generic, vars(typing).values()))
            )

    @staticmethod
    def generic(tp):
        generic = None
        if hasattr(tp, '_nparams'):
            orig = typing.get_origin(tp)
            generic = orig, tp
        return generic

    @classmethod
    def get_nparams(cls, tp):
        cls.make_builtin_types()
        tp = cls._BUILTIN_TYPES_COUNTERPARTS.get(tp, tp)
        return getattr(tp, '_nparams', None)


def factory_from_type_hint(hint, qualname=None):
    if hint is None:
        raise ValueError(
            'cannot deduce construct factory without type hint'
            + (' in ' + qualname if qualname else '')
        )
    if (
        isinstance(hint, (_Construct, _Subconstruct))
        or (isinstance(hint, type) and issubclass(hint, ConstructFactory))
    ):
        return hint
    tp = typing.get_origin(hint)
    [*args] = typing.get_args(hint)
    if tp and args:  # generic
        nparams = _TypingLib.get_nparams(tp)
        if nparams == -1:
            count = len(args)
            if ... in args:
                args.remove(...)
                count = None
        else:
            count = None
        [*remapped] = map(factory_from_type_hint, args)
        if issubclass(tp, types.UnionType):
            return _Construct(cs.Select(*remapped[::-1]))
        tp = PYTHON_GENERICS_AS_SUBCONSTRUCTS.get(tp, tp)
        if isinstance(tp, type) and issubclass(tp, ConstructFactory):
            return tp
        if not isinstance(tp, type):
            return tp(remapped, count=count)
        raise TypeError(f'{tp.__name__} type as a packet field type is not supported')
    basic_type = PYTHON_NON_GENERICS_AS_CONSTRUCTS.get(hint)
    if basic_type:
        return basic_type
    raise TypeError(f'cannot use ambiguous non-factory type {hint} as a packet field')


field_construct_callbacks = {}


def _call_field_construct(obj):
    try:
        construct = obj.construct
    except AttributeError:
        construct = field_construct_callbacks.get(id(obj))
    ret = None
    if construct:
        ret = construct()
    if ret is None:
        raise ValueError(f'{obj}.construct() is unknown or returned None')
    return ret


@dataclasses.dataclass
class _PacketPrivateEntries:
    packet: dataclasses.InitVar[PacketConstruct] = None
    entries: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self, packet: PacketConstruct):
        if packet is None:
            return
        self.set_packet(packet)

    def set_packet(self, packet: PacketConstruct):
        self.packet = weakref.ref(packet)

    def update(self, container: cs.Container):
        init = {}
        for key, value in container.items():
            if key.startswith('_'):
                self.entries[key] = value
            else:
                init[key] = value
        return init


@cs.singleton
class _Generic:
    def __call__(self, args, *, count=None):
        if len(args) == 1:
            subcon = factory_from_type_hint(*args)
        else:
            [*args] = map(_call_field_construct, map(factory_from_type_hint, args))
            if len(set(args)) > 1:
                return _Construct(cs.Sequence(*args))
            print(args)
            subcon = _Construct(args[0])
        if count is None:
            return _Construct(cs.GreedyRange(_call_field_construct(subcon)))
        return _Construct(cs.Array(count, _call_field_construct(subcon)))


class _Subconstruct:
    def __init__(self, api_name, construct, args=(), kwargs=None):
        self._api_name = api_name
        self._construct = construct
        self._args = args
        self._kwargs = kwargs or {}
        if __debug__:
            self._check_target_signature()

    def _check_target_signature(self):
        signature = inspect.signature(self._construct)
        try:
            signature.bind(*self._args, **self._kwargs)
        except TypeError as e:
            raise TypeError(
                f'too few arguments passed to {self._api_name}[]\n'
                f'Check help({self._construct.__module__}.{self._construct.__qualname__}) '
                'for details.'
            ) from e

    def __call__(self, *args, **kwargs):
        return self

    def construct(self):
        return self._construct(*self._args, **self._kwargs)


class _Construct:
    def __init__(self, construct, cast=None):
        self._construct = construct
        self._cast = cast

    def construct(self):
        return self._construct

    def __call__(self, obj):
        try:
            return self._cast(obj)
        except Exception:
            raise TypeError(f'cannot cast {obj} to desired type') from None

    def __getitem__(self, item):
        return Array[item, self._construct]

    def __repr__(self):
        return f'ConstructAPI({type(self._construct).__name__})'


Int8sl = _Construct(cs.Int8sl)
Int8sb = _Construct(cs.Int8sb)
Int8sn = _Construct(cs.Int8sn)
Int8ul = _Construct(cs.Int8ul, cast=ord)
Int8ub = _Construct(cs.Int8ub, cast=ord)
Int8un = _Construct(cs.Int8un, cast=ord)

Int16sl = _Construct(cs.Int16sl)
Int16sb = _Construct(cs.Int16sb)
Int16sn = _Construct(cs.Int16sn)
Int16ul = _Construct(cs.Int16ul, cast=ord)
Int16ub = _Construct(cs.Int16ub, cast=ord)
Int16un = _Construct(cs.Int16un, cast=ord)

Int24sl = _Construct(cs.Int24sl)
Int24sb = _Construct(cs.Int24sb)
Int24sn = _Construct(cs.Int24sn)
Int24ul = _Construct(cs.Int24ul)
Int24ub = _Construct(cs.Int24ub)
Int24un = _Construct(cs.Int24un)

Int32sl = _Construct(cs.Int32sl)
Int32sb = _Construct(cs.Int32sb)
Int32sn = _Construct(cs.Int32sn)
Int32ul = _Construct(cs.Int32ul)
Int32ub = _Construct(cs.Int32ub)
Int32un = _Construct(cs.Int32un)

Int64sl = _Construct(cs.Int64sl)
Int64sb = _Construct(cs.Int64sb)
Int64sn = _Construct(cs.Int64sn)
Int64ul = _Construct(cs.Int64ul)
Int64ub = _Construct(cs.Int64ub)
Int64un = _Construct(cs.Int64un)

Float64l = _Construct(cs.Float64l)
Float64b = _Construct(cs.Float64b)
Float64n = _Construct(cs.Float64n)


char = Int8sn
unsigned_char = Int8un


class PacketBase:
    def __class_getitem__(cls, args):
        if not isinstance(args, tuple):
            args = (args,)
        return cls._class_getitem(args)

    @classmethod
    def _class_getitem(cls, args):
        raise ValueError(f'{cls.__name__}[{", ".join(map(str, args))}] is undefined behaviour')


# @dataclasses.dataclass
class PacketConstruct(PacketBase):
    _private_entries: _PacketPrivateEntries
    _construct_class: typing.ClassVar[type[cs.Construct]]
    _skip_fields: typing.ClassVar[type[cs.Construct]] = []
    _environment: typing.ClassVar[dict | None] = None

    @classmethod
    def _setup_environment(cls, declaration_env):
        env = {}
        env.update(globals())
        env.update(vars(cls))
        if declaration_env:
            env.update(declaration_env)
        if cls._environment is None:
            cls._environment = {}
        cls._environment.update(env)

    def __init_subclass__(cls, **declaration_env):
        dataclasses.dataclass(cls)
        packet_fields = []

        cls._setup_environment(declaration_env)
        type_hints = typing.get_type_hints(cls, cls._environment)

        for field in dataclasses.fields(cls):  # noqa
            if field.name in cls._skip_fields:
                continue
            field_construct_callbacks[id(field)] = functools.partial(
                field_construct, field, type_hints.get(field.name)
            )
            packet_fields.append(field)

        setattr(cls, _PACKET_FIELDS, packet_fields)

    @classmethod
    def construct(cls):
        fields = map(_call_field_construct, getattr(cls, _PACKET_FIELDS))
        return cls._construct_class(*fields)

    def serialize(self, **context):
        construct = self.construct()
        src = dataclasses.asdict(self)  # noqa
        for skip_field in self._skip_fields:
            with contextlib.suppress(KeyError):
                del src[skip_field]
        return construct.build(src, **context)

    def __bytes__(self):
        return self.serialize()

    @classmethod
    def load(cls, data, **kwargs):
        construct = cls.construct()
        context = construct.parse(data)
        private_entries = _PacketPrivateEntries()
        init = private_entries.update(context)
        instance = cls(**init, **kwargs)
        private_entries.set_packet(instance)
        return instance


class PacketSubconstruct(PacketBase):
    _subconstruct_class = None

    @classmethod
    def construct(cls):
        raise TypeError(f'{cls.__name__} can only be used with []: {cls.__name__}[...]')

    @classmethod
    def _class_getitem(cls, args):
        return _Subconstruct(cls.__name__, cls._subconstruct_class, args)


class Struct(PacketConstruct):
    _construct_class = cs.Struct


class Array(PacketSubconstruct):
    _subconstruct_class = cs.Array


class LazyArray(PacketSubconstruct):
    _subconstruct_class = cs.LazyArray


PYTHON_NON_GENERICS_AS_CONSTRUCTS = {
    int: _Construct(cs.Int32sl),
    float: _Construct(cs.Float32l),
    str: _Construct(cs.CString(DEFAULT_COMMUNICATION_ENCODING)),
    bytes: _Construct(cs.GreedyBytes),
    bytearray: _Construct(cs.GreedyBytes),
}


PYTHON_GENERICS_AS_SUBCONSTRUCTS = dict.fromkeys({list, set, frozenset, tuple}, _Generic)


if __name__ == '__main__':

    class MyPacket(Struct):
        header: tuple[int, tuple[int, str]]

    pkt = MyPacket((1, (2, 'ez')))
    pickle = bytes(pkt)
    print(pickle)
    loaded = MyPacket.load(pickle)
    print(loaded)
