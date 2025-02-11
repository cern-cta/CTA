from .remote_host import RemoteHost

class CtaRmcdHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    def library_device(self) -> str:
        return self.execWithOutput("printenv LIBRARY_DEVICE")
