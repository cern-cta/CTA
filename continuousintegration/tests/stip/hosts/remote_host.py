from connections.remote_connection import RemoteConnection
from abc import ABC

class RemoteHost(ABC):
    def __init__(self, conn: RemoteConnection):
        self.conn = conn

    def run(self, command: str) -> bool:
        return self.conn.run(command)

    def copyTo(self, src_path: str, dst_path: str) -> bool:
        return self.conn.copyTo(src_path, dst_path)

    def copyFrom(self, src_path: str, dst_path: str) -> bool:
        return self.conn.copyFrom(src_path, dst_path)
