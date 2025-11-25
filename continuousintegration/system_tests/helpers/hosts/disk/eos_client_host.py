from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)
