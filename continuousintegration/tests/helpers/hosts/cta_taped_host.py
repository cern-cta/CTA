from .remote_host import RemoteHost

class CtaTapedHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    def drive_name(self) -> str:
        return self.execWithOutput("printenv DRIVE_NAME")

    def drive_device(self) -> str:
        return self.execWithOutput("printenv DRIVE_DEVICE")

    def library_device(self) -> str:
        return self.execWithOutput("printenv LIBRARY_DEVICE")

    def taped_log_file_location(self) -> str:
        return f"/var/log/cta/cta-taped-{self.drive_name()}.log"
