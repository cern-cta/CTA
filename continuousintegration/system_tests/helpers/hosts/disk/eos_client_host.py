from ..remote_host import RemoteHost
from .client_host import ClientHost
from typing import Protocol


class EosClientHost(RemoteHost, ClientHost, Protocol):
    def __init__(self, conn):
        super().__init__(conn)
