from .remote_host import RemoteHost

class EosMgmHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)
