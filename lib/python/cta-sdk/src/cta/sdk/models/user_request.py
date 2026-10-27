# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, ClassVar, Final, Literal, Optional

from cta.sdk.units import Byte

from .mount_type import MountType


@dataclass(eq=False)
class UserRequest:
    """Represent a request within CTA.

    Attributes:
        mount_type: Type of mount (read/write)
        tape_pool_name: Tape pool name
        vid: Tape volume identifier
        virtual_organisation_name: Virtual organisation name
        logical_library_name: Logical library name
        queued_file_count: Number of files queued
        queued_byte_count: Number of bytes queued
        oldest_age: Age of the oldest file in request
        priority: UserRequest priority
        min_age: Minimum age for files in request
        mount_count: Current number of mounts
        transferred_file_count: Current number of files mounted
        transferred_byte_count: Current number of bytes mounted
        is_sleeping_for_space: Whether request is sleeping for space
        sleep_start_time: Time when request started sleeping
        scheduler_backend_name: Scheduler backend name
        tapes_file_count: Total file count on tapes
        tapes_byte_count: Total byte count on tapes
        tapes_capacity: Total capacity of tapes
    """

    mount_type: Final[MountType]
    tape_pool_name: Final[str]
    vid: Final[Optional[str]]
    virtual_organisation_name: str
    logical_library_name: str
    queued_file_count: int
    queued_byte_count: Annotated[int, Byte]
    oldest_age: timedelta
    priority: int
    min_age: timedelta
    mount_count: int
    transferred_file_count: int
    transferred_byte_count: Annotated[int, Byte]
    is_sleeping_for_space: bool
    sleep_start_time: Optional[datetime]
    scheduler_backend_name: str
    tapes_file_count: int
    tapes_byte_count: Annotated[int, Byte]
    tapes_capacity: Annotated[int, Byte]

    def __hash__(self) -> int:
        return hash((self.mount_type, self.tape_pool_name, self.vid))

    def __eq__(self, other):
        if not isinstance(other, UserRequest):
            return False
        return (
            self.mount_type == other.mount_type
            and self.tape_pool_name == other.tape_pool_name
            and self.vid == other.vid
        )

    def __str__(self):
        return f"UserRequest({self.mount_type},{self.tape_pool_name if self.vid is None else self.vid})"


@dataclass
class UserRequestSummary(ABC):
    """Abstract Class to Grouped request information.

    Each concrete class has to defined `group_key` class attribute to work

    Attributes:
        group_value: Value of the grouping key
        min_age: Minimum age for files in grouped request
        oldest_age: Age of the oldest file in grouped request
        queued_file_count: Total queued file count in group
        queued_byte_count: Total queued byte count in group
        mount_count: Total current mount count in group
        transferred_file_count: Total current file count in group
        transferred_byte_count: Total current byte count in group
        tapes_count: Total tape count in group
        tapes_file_count: Total file count on tapes in group
        tapes_byte_count: Total byte count on tapes in group
        tapes_capacity: Total capacity of tapes in group
        highest_priority: Highest priority in group
    """

    _registry: ClassVar[dict[str, type["UserRequestSummary"]]] = {}
    group_key: ClassVar[str]
    """Key than subclasses need to specify"""

    def __post_init__(self):
        """Prevent instantiation of the abstract base class"""
        if type(self) is UserRequestSummary:
            raise TypeError("UserRequestSummary is abstract and cannot be instantiated")

    def __init_subclass__(cls) -> None:
        """
        Self register subclass to be requested from string
        with [group_by][cta.sdk.models.user_request.UserRequestSummary.group_by] method.
        """
        super().__init_subclass__()

        if cls.group_key in cls._registry:
            raise ValueError(f"Duplicate group_key: {cls.group_key}")

        cls._registry[cls.group_key] = cls

    group_value: str
    min_age: timedelta = timedelta(seconds=1_000_000_000)
    oldest_age: timedelta = timedelta()
    queued_file_count: int = 0
    queued_byte_count: Annotated[int, Byte] = 0
    mount_count: int = 0
    transferred_file_count: int = 0
    transferred_byte_count: Annotated[int, Byte] = 0
    tapes_count: Optional[int] = None
    tapes_file_count: int = 0
    tapes_byte_count: Annotated[int, Byte] = 0
    tapes_capacity: Annotated[int, Byte] = 0
    highest_priority: int = 0

    @classmethod
    def group_by(
        cls, key: Literal["tape_pool_name", "logical_library_name", "virtual_organisation_name"]
    ) -> type["UserRequestSummary"]:
        """Get a real UserRequestSummary with the corresponding grouping key set.

        Returns:
            type["UserRequestSummary"]: Concrete UserRequestSummary with `group_key` set
        """
        return cls._registry[key]


@dataclass
class UserRequestSummaryByTapePool(UserRequestSummary):
    """User Request Summary with "tape_pool_name" as `group_key`"""

    group_key: ClassVar[str] = "tape_pool_name"
    """`tape_pool_name` as grouping key"""


@dataclass
class UserRequestSummaryByLogicalLibrary(UserRequestSummary):
    group_key: ClassVar[str] = "logical_library_name"
    """`logical_library_name` as grouping key"""


@dataclass
class UserRequestSummaryByVirtualOrganisation(UserRequestSummary):
    group_key: ClassVar[str] = "virtual_organisation_name"
    """`virtual_organisation_name` as grouping key"""
