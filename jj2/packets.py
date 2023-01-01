"""
Introducing packets:
data-holding classes that can be serialized into bytes and then recreated from them.

Unlike pickling, this process is guaranteed to be safe and is fully designed by the programmer.

Works with the _construct_ library behind the scenes.
"""

from __future__ import annotations

import abc
import contextlib
import dataclasses
import functools
import inspect
import types
import typing
import weakref

import construct as cs

from jj2.constants import DEFAULT_COMMUNICATION_ENCODING


class ConstructFactory(metaclass=abc.ABCMeta):
    @classmethod
    def construct(cls) -> cs.Construct:
        """Return a construct for self-serialization and self-deserialization."""
        raise NotImplementedError


_PACKET_FIELDS = '__packet_fields__'


def field_construct(packet_field, fallback_type_hint, cls_name=None):
    name = packet_field.name
    qualname = (cls_name + '.' if cls_name else '') + (name or '')
    packet_field.metadata = dict(packet_field.metadata)
    construct_factory = (
        packet_field.metadata.get('construct_factory')
        or packet_field.metadata.setdefault(
            'construct_factory',
            deduce_factory(fallback_type_hint, qualname)
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


def deduce_factory(python_type, qualname=None):
    if python_type is None:
        raise ValueError(
            'cannot deduce construct factory without type hint'
            + (' in ' + qualname if qualname else '')
        )
    tp = typing.get_origin(python_type)
    args = list(typing.get_args(python_type))
    if (
        isinstance(python_type, (_Construct, _Subconstruct))
        or (
            isinstance(python_type, type)
            and not args  # for some reason abc subclass checks raise for generic types
            and issubclass(python_type, ConstructFactory)
        )
    ):
        return python_type
    if tp and args:
        nparams = _TypingLib.get_nparams(tp)
        if nparams == -1:
            count = len(args)
            if ... in args:
                args.remove(...)
                count = None
        else:
            count = None
        factories = list(map(deduce_factory, args))
        if issubclass(tp, types.UnionType):
            return _Construct(cs.Select(*factories[::-1]))
        tp = PYTHON_GENERICS_AS_SUBCONSTRUCTS.get(tp, tp)
        if isinstance(tp, type) and issubclass(tp, ConstructFactory):
            return tp
        if not isinstance(tp, type):
            return tp(factories, count=count)
        raise TypeError(f'{tp.__name__} type as a packet field type is not supported')
    basic_type = PYTHON_NON_GENERICS_AS_CONSTRUCTS.get(python_type)
    if basic_type:
        return basic_type
    raise TypeError(f'cannot use ambiguous non-factory type {python_type} as a packet field')


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
    _packet = None

    def __post_init__(self, packet: PacketConstruct):
        if packet is None:
            return
        self.set_packet(packet)

    def set_packet(self, packet: PacketConstruct):
        self.entries.clear()
        self._packet = weakref.ref(packet)

    def update(self, container: cs.Container):
        init = {}
        for key, value in container.items():
            if key.startswith('_'):
                self.entries[key] = value
            else:
                init[key] = value
        return init


def ensure_construct(obj):
    return _call_field_construct(deduce_factory(obj))


class _Generic:
    def __init__(self, python_type=None):
        self._python_type = python_type

    def __call__(self, args, *, count=None):
        if len(args) == 1:
            subcon = deduce_factory(*args)
        else:
            [*args] = map(ensure_construct, args)
            if len(set(args)) > 1:
                return _Construct(cs.Sequence(*args), python_type=self._python_type)
            subcon = _Construct(args[0])
        if count is None:
            return _Construct(
                cs.GreedyRange(_call_field_construct(subcon)),
                python_type=self._python_type
            )
        return _Construct(
            cs.Array(count, _call_field_construct(subcon)),
            python_type=self._python_type
        )


class _Subconstruct(ConstructFactory):
    def __init__(
            self,
            api_name,
            construct,
            args=(),
            kwargs=None,
            python_type=None
    ):
        self._api_name = api_name
        self._do_construct = construct
        self._args = args
        self._kwargs = kwargs or {}
        self._python_type = python_type
        if __debug__:
            self._check_params()

    def _check_params(self):
        signature = inspect.signature(self._do_construct)
        try:
            signature.bind(*self._args, **self._kwargs)
        except TypeError as e:
            raise TypeError(
                f'erroneous arguments passed to {self._api_name}[]\n'
                f'Check help({self._do_construct.__module__}.{self._do_construct.__qualname__}) '
                'for details.'
            ) from e

    def __call__(self, *args, **kwargs):
        return self

    def __getitem__(self, size):
        return Array[size, self._do_construct]

    def construct(self):
        return _construct_force_cast(
            self._python_type, self._do_construct(*self._args, **self._kwargs)
        )


def _construct_force_cast(python_type, construct):
    if python_type and not getattr(construct, '_implies_type_coercion', False):
        parsereport = construct._parsereport
        construct._implies_type_coercion = True
        construct._parsereport = lambda stream, context, path: python_type(
            parsereport(stream, context, path)
        )
    return construct


class _Construct(ConstructFactory):
    def __init__(self, construct, cast=None, python_type=None):
        self._construct = construct
        self._cast = cast
        self._python_type = python_type

    def construct(self):
        return _construct_force_cast(self._python_type, self._construct)

    def __call__(self, obj):
        try:
            return self._cast(obj)
        except Exception:
            raise TypeError(f'cannot cast {obj} to desired type') from None

    def __getitem__(self, size):
        return Array[size, self._construct]

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


class PacketBase(ConstructFactory, metaclass=abc.ABCMeta):
    def __class_getitem__(cls, args):
        if not isinstance(args, tuple):
            args = (args,)
        return cls._class_getitem(args)

    @classmethod
    def _class_getitem(cls, args):
        raise ValueError(f'{cls.__name__}[{", ".join(map(str, args))}] is undefined behaviour')


def make_inner_of(inner_cls, wrapper_cls, /, **kwds):
    class BoundSubconstruct(PacketBase):
        def __init__(self, *args, **kwargs):
            self._data_for_building = wrapper_cls.init(inner_cls, self, *args, **kwargs)

        def _get_data_for_building(self):
            return self._data_for_building

        @classmethod
        def construct(cls):
            return wrapper_cls.subconstruct(subcon=ensure_construct(inner_cls), **kwds)

        def build(self, **context):
            construct = self.construct()
            return construct.build(self._get_data_for_building(), **context)

        def __bytes__(self):
            return self.build()

        def __iter__(self):
            yield from wrapper_cls.iter(self)

        @classmethod
        def load(cls, data, **kwargs):
            construct = cls.construct()
            context = construct.parse(data)
            instance = wrapper_cls.load(cls, inner_cls, context, **kwargs)
            return instance

        @classmethod
        def make_inner_of(cls, outer_wrapper_cls, /, **kwargs):
            return make_inner_of(cls, outer_wrapper_cls, **kwargs)

        def __repr__(self):
            return wrapper_cls.repr(self, inner_cls, **kwds)

    return BoundSubconstruct


class PacketConstruct(PacketBase):
    _construct_class = None  # type: typing.ClassVar[type[ConstructFactory] | None]
    _environment = None  # type: typing.ClassVar[dict[str, typing.Any] | None]
    _skip_fields = []  # type: typing.ClassVar[list[str]]

    @classmethod
    def _setup_environment(cls, declaration_env):
        env = {}
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

    def _get_data_for_building(self):
        data = dataclasses.asdict(self)  # noqa
        for skip_field in self._skip_fields:
            with contextlib.suppress(KeyError):
                del data[skip_field]
        return data

    def build(self, **context):
        construct = self.construct()
        return construct.build(self._get_data_for_building(), **context)

    def __bytes__(self):
        return self.build()

    def __iter__(self):
        yield from self._get_data_for_building()

    @classmethod
    def load(cls, data, **kwargs):
        construct = cls.construct()
        args = construct.parse(data)
        return cls._load_from_args(args, **kwargs)

    @classmethod
    def _load_from_args(cls, args, **kwargs):
        private_entries = _PacketPrivateEntries()
        init = private_entries.update(args)
        instance = cls(**init, **kwargs)
        private_entries.set_packet(instance)
        return instance

    @classmethod
    def make_inner_of(cls, wrapper_cls, /, **kwds):
        return make_inner_of(cls, wrapper_cls, **kwds)


class Struct(PacketConstruct):
    _construct_class = cs.Struct


class BitStruct(PacketConstruct):
    _construct_class = cs.BitStruct


def field(name, construct_factory, **kwargs) -> dataclasses.Field:
    metadata = kwargs.setdefault('metadata', {})
    metadata.update(construct_factory=construct_factory)
    f = dataclasses.field(**kwargs)
    f.name = name
    return f


MISSING_EXTENDS = object()


class Sequence(PacketConstruct):
    _construct_class = cs.Sequence
    fields = None

    @classmethod
    def _load_from_args(cls, args, **kwargs):
        instance = cls(*args, **kwargs)
        return instance

    @classmethod
    def _autocreate_field_name(cls, f, i):
        return f'field_{i}'

    def _get_data_for_building(self):
        return list(super()._get_data_for_building().values())

    def __getitem__(self, item):
        return list(self)[item]

    def __iter__(self):
        yield from self._get_data_for_building()

    def __init_subclass__(cls, extends=MISSING_EXTENDS, **env):
        if extends is MISSING_EXTENDS:
            extends = cls.__base__
        super_fields = (
            (getattr(extends, 'fields', None) or ())
            if extends is not None else ()
        )
        fields = [*super_fields, *(getattr(cls, 'fields') or ())]

        orig_annotations = cls.__annotations__
        cls.__annotations__ = {
            name: (
                f.metadata.get('construct_factory') or f.type
                if isinstance(f, dataclasses.Field) else f
            )
            for i, f in enumerate(fields, start=1)
            if (name := getattr(f, 'name', cls._autocreate_field_name(f, i))) != 'fields'
        }
        env.update(vars(cls))

        try:
            super().__init_subclass__(**env)
        finally:
            cls.__annotations__ = orig_annotations


class PacketSubconstruct(PacketBase):
    _subconstruct_factory = None

    @classmethod
    def construct(cls):
        raise TypeError(f'{cls.__name__} can only be used with []: {cls.__name__}[...]')

    @classmethod
    def _class_getitem(cls, args):
        return cls.subconstruct(args)

    @classmethod
    def subconstruct(cls, *args, **kwargs):
        return _Subconstruct(cls.__name__, cls._subconstruct_factory, args, kwargs).construct()

    @staticmethod
    def map_kwargs(kwargs):
        return kwargs

    @staticmethod
    def init(inner_cls, instance):
        pass

    @staticmethod
    def load(outer_cls, inner_cls, context, **kwargs):
        pass

    @staticmethod
    def repr(instance, inner_cls, **kwds):
        return object.__repr__(instance)

    @staticmethod
    def iter(instance):
        yield from instance._get_data_for_building()

    @classmethod
    def of(cls, packet=None, **kwargs):
        if packet is None:
            return functools.partial(cls.of, **kwargs)
        return packet.make_inner_of(cls, **kwargs)


class _HomogeneousCollectionSubconstruct(PacketSubconstruct):
    @staticmethod
    def init(inner_cls, instance, *inits):
        instance._args = [
            (
                init if isinstance(init, inner_cls) else
                (inner_cls(**init) if isinstance(init, dict) else inner_cls(*init))
            )
            for init in inits
        ]
        return [member._get_data_for_building() for member in instance._args]

    @staticmethod
    def load(outer_cls, inner_cls, context, **kwargs):
        return outer_cls(*(
            inner_cls._load_from_args(subcontext, **kwargs)
            for subcontext in context
        ))

    @classmethod
    def repr(cls, instance, inner_cls, **kwds):
        return (
            cls.__name__
            + ', '.join(
                filter(None, (inner_cls.__name__, ', '.join(
                    f'{key!s}={value!r}'
                    for key, value in kwds.items()
                )))
            ).join('<>')
            + ', '.join(map(repr, instance._args)).join('()')
        )

    @staticmethod
    def iter(instance):
        yield from instance._args


class Array(_HomogeneousCollectionSubconstruct):
    _subconstruct_factory = cs.Array

    @classmethod
    def _class_getitem(cls, args):
        return cls.subconstruct(args)


class LazyArray(_HomogeneousCollectionSubconstruct):
    _subconstruct_factory = cs.LazyArray


class GreedyRange(_HomogeneousCollectionSubconstruct):
    _subconstruct_factory = cs.GreedyRange


class Prefixed(PacketSubconstruct):
    _subconstruct_factory = cs.Prefixed

    @staticmethod
    def map_kwargs(kwargs):
        kwargs.update(lengthfield=ensure_construct(kwargs.get('lengthfield')))
        return kwargs


class Rebuild(PacketSubconstruct):
    _subconstruct_factory = cs.Rebuild


class Default(PacketSubconstruct):
    _subconstruct_factory = cs.Default


class Optional(PacketSubconstruct):
    _subconstruct_factory = cs.Optional


class Pointer(PacketSubconstruct):
    _subconstruct_factory = cs.Pointer


class Peek(PacketSubconstruct):
    _subconstruct_factory = cs.Peek


class Padded(PacketSubconstruct):
    _subconstruct_factory = cs.Padded


PYTHON_NON_GENERICS_AS_CONSTRUCTS = {
    int: _Construct(cs.Int32sl),
    float: _Construct(cs.Float32l),
    str: _Construct(cs.CString(DEFAULT_COMMUNICATION_ENCODING)),
    bytes: _Construct(cs.GreedyBytes),
    bytearray: _Construct(cs.GreedyBytes, python_type=bytearray),
}


PYTHON_GENERICS_AS_SUBCONSTRUCTS = {
    list: _Generic(python_type=list),
    set: _Generic(python_type=set),
    frozenset: _Generic(python_type=frozenset),
    tuple: _Generic(python_type=tuple),
}
