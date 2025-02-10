from typing import Protocol
from typing import Optional

from subprocess import CompletedProcess

class RemoteConnection(Protocol):

    def get_name(self) -> str:
        ...

    def get_short_description(self) -> str:
        ...

    def exec(self, command: str, capture_output = False, throw_on_failure = True) -> CompletedProcess[bytes]:
        ...

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure = True, permissions: Optional[str] = None) -> None:
        ...

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure = True) -> None:
        ...
