from .remote_host import RemoteHost

class CtaCliHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)
