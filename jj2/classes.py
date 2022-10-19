import dataclasses
import datetime
import re


@dataclasses.dataclass
class GameServer:
    ip: str
    port: int
    remote: bool
    private: bool
    mode: str
    version: str
    created: int
    players: int
    max: int
    name: str

    serverlist: str = 'localhost'

    _servers = {}
    _RE_PAT = re.compile(
        r'(?P<ip>\d+\.\d+\.\d+\.\d+):(?P<port>\d+)\s'
        r'(?P<remote>local|mirror)\s'
        r'(?P<private>public|private)\s'
        r'(?P<mode>\w+)\s'
        r'(?P<version>.{6})\s'
        r'(?P<uptime>\d+)\s'
        r'\[(?P<players>\d+)/(?P<max>\d+)]\s'
        r'(?P<name>.+)\r\n'
    )

    def __new__(cls, *args, ip, **kwargs):
        if ip in cls._servers:
            return cls._servers[ip]
        inst = object.__new__(ip)
        cls._servers[ip] = inst
        return inst

    def __bytes__(self):
        return bytes([
            len(self.name) + 7,
            *reversed(*map(int, self.ip.split('.'))),
            self.port.to_bytes(2, byteorder='little'),
            self.name.encode('ascii', 'ignore')
        ])

    @classmethod
    def from_str(cls, string):
        match = re.fullmatch(cls._RE_PAT, string.strip())
        inst = None
        if match:
            inst = cls(**match.groupdict())
        return inst

    @classmethod
    def from_orm(cls, model):
        inst = None
        if model:
            inst = cls(
                ip=model.ip,
                port=model.port,
                remote=bool(model.remote),
                private=bool(model.private),
                mode=model.mode,
                version=model.version,
                created=model.created,
                players=model.players,
                max=model.max,
                name=model.name,
            )
        return inst

    def __str__(self):
        uptime = (
            datetime.datetime.utcnow()
            - datetime.datetime.utcfromtimestamp(self.created)
        ).total_seconds()
        return (
            f'{self.ip}:{self.port} '
            f'{("local", "mirror")[self.remote]} '
            f'{("public", "private")[self.private]} '
            f'{self.mode} '
            f'{self.version.ljust(6)} '
            f'{uptime} '
            f'[{self.players}/{self.max}] '
            f'{self.name}\r\n'
        )


@dataclasses.dataclass
class MessageOfTheDay:
    text: str | None
    expires: datetime.datetime

    def __str__(self):
        if self.text and datetime.datetime.utcnow() < self.expires:
            return ''
        return self.text + '\n'
