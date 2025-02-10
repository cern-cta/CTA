from .remote_host import RemoteHost

class CtaFrontendHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    def get_schema_version(self) -> str:
        return self.execWithOutput("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf | grep -o -E '[0-9]+\.[0-9]")
