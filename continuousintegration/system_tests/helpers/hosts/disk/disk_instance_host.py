from typing import Protocol


class DiskInstanceHost(Protocol):

    @property
    def instance_name(self) -> str:
        ...

    def force_remove_directory(self, directory: str) -> str:
        ...

    def num_files_in_directory(self, directory: str) -> str:
        ...

    def num_files_on_tape_only(self, directory: str) -> str:
        ...

    def num_files_on_disk_only(self, directory: str) -> str:
        ...
