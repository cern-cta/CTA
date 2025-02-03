from .remote_host import RemoteHost

class ClientHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)
