from .remote_host import RemoteHost
from functools import cached_property

class CtaFrontendHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def log_file_location(self) -> str:
        return f"/var/log/cta/cta-frontend.log"

    def get_schema_version(self) -> str:
        return self.execWithOutput(r"cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf | grep -o -E '[0-9]+\.[0-9]'")
