from __future__ import annotations

import dataclasses
import datetime
import ipaddress
import re
import sys
import typing

import construct as cs

from jj2.constants import DEFAULT_GAME_SERVER_PORT

if typing.TYPE_CHECKING:
    from typing import ClassVar
    from typing import Hashable


class GameServer:
    name: str = 'unknown'
    remote: bool = False
    private: bool = False
    mode: str = 'capture'
    version: str = '1.24'
    listed_at: datetime.datetime | None = None
    clients: int | None = None
    max_clients: int | None = None
    listserver: str | None = None
    plus_version: str | None = None
    isolated: bool = False

    def __init__(
        self,
        ip_address: str | int | bytes | ipaddress.IPv4Address | ipaddress.IPv6Address,
        port: int = DEFAULT_GAME_SERVER_PORT, *,
        name: str | None = None,
        remote: bool | None = None,
        private: bool | None = None,
        mode: str | None = None,
        version: str | None = None,
        listed_at: datetime.datetime | None = None,
        clients: int | None = None,
        max_clients: int | None = None,
        listserver: str | None = None,
        plus_version: str | None = None,
        isolated: bool = False,
    ):
        """
        Parameters
        ----------
        ip_address : str or int or bytes or ipaddress.IPv4Address or ipaddress.IPv6Address
            The IPv4 or IPv6 address of this server.
        port : int
            The network port of this server.
        remote : bool or None
            States whether this server was first listed on the assigned :attribute:`listserver`.
        private : bool or None
            States whether this server requires password from incoming clients.
        mode : bool or None
            Game mode of the running game server.
        version : str or None
            Game version of this server.
        listed_at : datetime.datetime or None
            States when this server was listed.
            Defaults to the UTC datetime of the __init__() call.
        clients : int
            Number of JJ2 clients currently being in this server.
        max_clients : int
            Maximal number of clients to be connected to this server.
        name : str
            The server name.
        listserver  : str
            A last listserver that affected this object.
        plus_version : str
            Version of JJ2+ (the 'plus' JJ2 modification) that this server runs.
        isolated : bool
            Construct with isolated=True if you need to isolate the new instance from already
            existing  GameServer object with same IP and port attributes, for any reason.
            If the new GameServer instance is isolated, no strong reference is made to it
            in the :attribute:`_servers` attribute.
        """
        self.ip_address = ipaddress.ip_address(ip_address)
        self.port = int(port)
        self.isolated = isolated
        if listed_at is None:
            listed_at = self.listed_at or datetime.datetime.utcnow()
        self.listed_at = listed_at
        if name:
            self.name = name
        if mode:
            self.mode = mode
        if version:
            self.version = version
        if listserver:
            self.listserver = listserver
        if plus_version:
            self.plus_version = plus_version
        if remote is not None:
            self.remote = remote
        if private is not None:
            self.private = private
        if clients is not None:
            self.clients = clients
        if max_clients is not None:
            self.max_clients = max_clients

    @classmethod
    def get_instance_key(
            cls, ip_address, port=DEFAULT_GAME_SERVER_PORT, *_args, **_kwargs
    ) -> Hashable:
        """Get this instance's key for lookup."""
        ip_address = ipaddress.ip_address(ip_address).compressed
        return sys.intern(f'{ip_address}:{port}')

    _servers: ClassVar[dict[Hashable, GameServer]] = {}

    def __new__(cls, *args, isolated: bool = False, **kwargs) -> GameServer:
        key = cls.get_instance_key(*args, **kwargs)
        if not isolated and key in cls._servers:
            return cls._servers[key]
        inst = object.__new__(cls)
        if not isolated:
            cls._servers[key] = inst
        return inst

    _ASCIILIST_REPR_PATTERN: re.Pattern = re.compile(
        r'(?P<ip_address>[\dabcdef:.]+):(?P<port>\d+)\s'
        r'(?P<remote>local|mirror)\s'
        r'(?P<private>public|private)\s'
        r'(?P<mode>\w+)\s'
        r'(?P<version>.{6})\s'
        r'(?P<uptime>\d+)\s'
        r'\[(?P<clients>\d+)/(?P<max_clients>\d+)]\s'
        r'(?P<name>.+)(\r\n)?'
    )

    _IPV4_BINARYLIST_REPR_PATTERN: cs.Construct = cs.Struct(
        ip_address=cs.ByteSwapped(cs.Bytes(4)),
        port=cs.Int16ul,
        name=cs.ExprValidator(cs.GreedyBytes, lambda obj, ctx: obj.isascii()),
    ).compile()

    _IPV6_BINARYLIST_REPR_PATTERN: cs.Construct = cs.Struct(
        ip_address=cs.ByteSwapped(cs.Bytes(16)),
        port=cs.Int16ul,
        name=cs.ExprValidator(cs.GreedyBytes, lambda obj, ctx: obj.isascii()),
    ).compile()

    _BINARYLIST_REPR_PATTERN: cs.Construct = cs.GreedyRange(
        cs.Select(
            cs.Prefixed(cs.Byte, _IPV4_BINARYLIST_REPR_PATTERN, includelength=True),
            cs.Prefixed(cs.Byte, _IPV6_BINARYLIST_REPR_PATTERN, includelength=True),
        )
    ).compile()

    def dict(self):
        return {
            'name': self.name,
            'remote': self.remote,
            'private': self.private,
            'mode': self.mode,
            'version': self.version,
            'listed_at': self.listed_at,
            'clients': self.clients,
            'max_clients': self.max_clients,
            'listserver': self.listserver,
            'plus_version': self.plus_version,
            'isolated': self.isolated,
        }

    @property
    def binarylist_repr(self) -> bytes:
        return self._BINARYLIST_REPR_PATTERN.build([dict(
            ip_address=self.ip_address.packed,
            port=self.port,
            name=self.name.encode()
        )])

    @classmethod
    def from_binarylist_reprs(cls, binarylist_repr: bytes, isolated: bool = False):
        data = cls._BINARYLIST_REPR_PATTERN.parse(binarylist_repr)
        return [cls(
            ip_address=chunk.ip_address,
            port=chunk.port,
            name=chunk.name.decode(),
            isolated=isolated
        ) for chunk in data]

    @property
    def asciilist_repr(self):
        uptime = int((datetime.datetime.utcnow() - self.listed_at).total_seconds())
        return (
            f'{self.ip_address.compressed}:{self.port} '
            f'{("local", "mirror")[self.remote]} '
            f'{("public", "private")[self.private]} '
            f'{self.mode} '
            f'{self.version.ljust(6)} '
            f'{uptime} '
            f'[{self.clients or 0}/{self.max_clients or 32}] '
            f'{self.name}\r\n'
        )

    @classmethod
    def from_asciilist_repr(cls, string: str, isolated: bool = False):
        match = re.fullmatch(cls._ASCIILIST_REPR_PATTERN, string.strip())
        inst = None
        if match:
            info = match.groupdict()
            info.update(
                remote=info['remote'] == 'mirror',
                private=info['private'] == 'private',
                version=info['version'].strip(),
                clients=int(info['clients']),
                max_clients=int(info['max_clients']),
                listed_at=(
                    datetime.datetime.utcnow()
                    - datetime.timedelta(seconds=int(info.pop('uptime')))
                )
            )
            inst = cls(**info, isolated=isolated)
        return inst

    def __repr__(self):
        class_name = type(self).__name__
        addr, port, name = self.ip_address, self.port, self.name
        return f'<{class_name} [{self.clients}/{self.max_clients}] {addr=!r} {port=!s} {name=!r}>'


@dataclasses.dataclass
class MessageOfTheDay:
    text: str | None = None
    expires: datetime.datetime | None = None
    updated: datetime.datetime = dataclasses.field(default_factory=datetime.datetime.utcnow)

    def dict(self):
        return dataclasses.asdict(self)

    def __str__(self):
        if self.text and datetime.datetime.utcnow() < self.expires:
            return ''
        return (self.text or '') + '\n'


@dataclasses.dataclass
class BanlistEntry:
    address: str
    type: str
    note: str
    origin: str
    reserved: str

    def dict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Mirror:
    name: str
    address: str

    def dict(self):
        return dataclasses.asdict(self)
