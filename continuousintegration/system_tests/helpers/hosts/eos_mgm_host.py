from .remote_host import RemoteHost
from functools import cached_property


class EosMgmHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def instance_name(self) -> str:
        return self.execWithOutput("eos --json version | jq -r '.version[0].EOS_INSTANCE'")

    def force_remove_directory(self, directory: str) -> str:
        self.exec(f"eos rm -rF --no-confirmation {directory} 2>/dev/null || true")
