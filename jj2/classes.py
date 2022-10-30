import dataclasses
import datetime
import ipaddress
import re
import sys
import typing

import construct as cs

if typing.TYPE_CHECKING:
    from typing import ClassVar
    from typing import Hashable
    from jj2.listservers.db.models import ServerModel


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

    _servers: 'ClassVar[dict[Hashable, GameServer]]' = {}

    _ASCIILIST_REPR_PATTERN: re.Pattern = re.compile(
        r'(?P<ip>\d+\.\d+\.\d+\.\d+):(?P<port>\d+)\s'
        r'(?P<remote>local|mirror)\s'
        r'(?P<private>public|private)\s'
        r'(?P<mode>\w+)\s'
        r'(?P<version>.{6})\s'
        r'(?P<uptime>\d+)\s'
        r'\[(?P<players>\d+)/(?P<max>\d+)]\s'
        r'(?P<name>.+)\r\n'
    )
    _NAME_ENCODING: str = 'ASCII'

    _IPV4_BINARYLIST_REPR_PATTERN = cs.Struct(
        ip=cs.ByteSwapped(cs.Bytes(4)),
        port=cs.Int16ul,
        name=cs.GreedyString(_NAME_ENCODING),
        eof=cs.Terminated
    )

    _IPV6_BINARYLIST_REPR_PATTERN = cs.Struct(
        ip=cs.ByteSwapped(cs.Bytes(16)),
        port=cs.Int16ul,
        name=cs.GreedyString(_NAME_ENCODING),
        eof=cs.Terminated
    )

    _BINARYLIST_REPR_PATTERN: cs.Construct = cs.Select(
        cs.Prefixed(cs.Byte, _IPV4_BINARYLIST_REPR_PATTERN, includelength=True),
        cs.Prefixed(cs.Byte, _IPV6_BINARYLIST_REPR_PATTERN, includelength=True),
    )

    @classmethod
    def get_instance_key(cls, ip, port=10052, *_args, **_kwargs) -> 'Hashable':
        """Get this instance's key for lookup."""
        return sys.intern(f'{ip}:{port}')

    def __new__(cls, *args, isolated=False, **kwargs) -> 'GameServer':
        key = cls.get_instance_key(*args, **kwargs)
        if not isolated and key in cls._servers:
            return cls._servers[key]
        inst = object.__new__(cls)
        if not isolated:
            cls._servers[key] = inst
        return inst

    def __init__(
        self,
        ip: str | int | bytes | ipaddress.IPv4Address | ipaddress.IPv6Address,
        port: int = 10052, *,
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
        ip : str or int or bytes or ipaddress.IPv4Address or ipaddress.IPv6Address
            The IP (v4 or v6) address of this server.
        port : int = 10052
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
        self.ip = ipaddress.ip_address(ip)
        self.port = port
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
    def from_orm(cls, model: 'ServerModel', serverlist: str | None = None) -> 'GameServer':
        inst = None
        if model:
            inst = cls(
                ip=ipaddress.ip_address(model.ip),
                port=model.port,
                remote=bool(model.remote),
                private=bool(model.private),
                mode=model.mode,
                version=model.version,
                listed_at=datetime.datetime.utcfromtimestamp(model.created),
                clients=model.players,
                max_clients=model.max,
                name=model.name,
            )
        if serverlist:
            inst.listserver = serverlist
        return inst

    @property
    def binarylist_repr(self) -> bytes:
        return self._BINARYLIST_REPR_PATTERN.build(dict(
            ip=self.ip.packed,
            port=self.port,
            name=self.name
        ))

    @classmethod
    def from_binarylist_repr(cls, binarylist_repr: bytes, isolated: bool = False):
        data = cls._BINARYLIST_REPR_PATTERN.parse(binarylist_repr)
        return cls(
            ip=data.ip,
            port=data.port,
            name=data.name,
            isolated=isolated
        )

    @property
    def asciilist_repr(self):
        uptime = (datetime.datetime.utcnow() - self.listed_at).total_seconds()
        return (
            f'{self.ip}:{self.port} '
            f'{("local", "mirror")[self.remote]} '
            f'{("public", "private")[self.private]} '
            f'{self.mode} '
            f'{self.version.ljust(6)} '
            f'{uptime} '
            f'[{self.clients}/{self.max_clients}] '
            f'{self.name}\r\n'
        )

    @classmethod
    def from_asciilist_repr(cls, string: str, isolated: bool = False):
        match = re.fullmatch(cls._ASCIILIST_REPR_PATTERN, string.strip())
        inst = None
        if match:
            inst = cls(**match.groupdict(), isolated=isolated)
        return inst


@dataclasses.dataclass
class MessageOfTheDay:
    text: str | None
    expires: datetime.datetime

    def __str__(self):
        if self.text and datetime.datetime.utcnow() < self.expires:
            return ''
        return self.text + '\n'


@dataclasses.dataclass
class BanlistEntry:
    address: str
    type: str
    note: str
    origin: str
    reserved: str
