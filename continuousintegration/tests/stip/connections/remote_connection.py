from typing import Protocol

class RemoteConnection(Protocol):
    def run(self, command: str) -> bool:
        ...

    def copyTo(self, src_path: str, dst_path: str) -> bool:
        ...

    def copyFrom(self, src_path: str, dst_path: str) -> bool:
        ...
