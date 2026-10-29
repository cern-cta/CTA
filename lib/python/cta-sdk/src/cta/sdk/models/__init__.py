# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Subpackage holding all dataclass models

Dataclass models represents CTA resource or sub information of CTA resource.
Most of resources are mapped one to one with what is present in the catalogue
but some of them represent object store resource like RepackRequest.

Warning:
    The attributes typed with Final keyword represents the primary (composite or not) key of
    the object. These attributes are the one used to compute the hash of the object and to
    the equality method. So technically, two objects can be different in terms of all their
    attributes but still equals through `==` operator.

Info:
    Most of dataclasses are not supposed to be instanciate manually, but being created
    or retrieved through its corresponding dao. So the situation describe aboved should not
    happen.
"""

from .activity_mount_rule import ActivityMountRule
from .admin_user import AdminUser
from .archive_file import ArchiveFile, Checksum, DiskFile
from .archive_route import ArchiveRoute, ArchiveRouteType
from .cta_version import CtaCliVersion, CtaServerVersion, CtaVersion
from .disk_instance import DiskInstance
from .disk_instance_space import DiskInstanceSpace
from .disk_system import DiskSystem
from .drive import Drive, DriveConfig, DriveMetadata, DriveOperatingState
from .failed_request import FailedRequest, FailedRequestSummary
from .group_mount_rule import GroupMountRule
from .logical_library import LogicalLibrary, LogicalLibraryState
from .media_type import MediaType
from .metadata import Metadata
from .mount_policy import MountPolicy
from .mount_type import MountType
from .physical_library import PhysicalLibrary, PhysicalLibraryState
from .recycle_bin_tape_file import RecycleBinTapeFile
from .repack_request import (
    RepackRequest,
    RepackStartInfo,
    RepackState,
    RepackStatistics,
)
from .storage_class import StorageClass
from .tape import Tape, TapeMetadata, TapeState, TapeStatistics, VerificationStatus
from .tape_file import TapeFile
from .tape_pool import TapePool, TapePoolStatistics
from .tape_session import TapeSession, TapeSessionState
from .user_mount_rule import UserMountRule
from .user_request import UserRequest, UserRequestSummary
from .virtual_organisation import VirtualOrganisation

__all__ = [
    "Metadata",
    "AdminUser",
    "ArchiveFile",
    "CtaVersion",
    "CtaCliVersion",
    "CtaServerVersion",
    "VirtualOrganisation",
    "DiskInstance",
    "DiskInstanceSpace",
    "DiskSystem",
    "Checksum",
    "DiskFile",
    "TapeFile",
    "RecycleBinTapeFile",
    "StorageClass",
    "Drive",
    "DriveMetadata",
    "DriveOperatingState",
    "DriveConfig",
    "TapeSession",
    "TapeSessionState",
    "MediaType",
    "PhysicalLibrary",
    "PhysicalLibraryState",
    "Tape",
    "TapeState",
    "TapeMetadata",
    "TapeStatistics",
    "VerificationStatus",
    "TapePool",
    "TapePoolStatistics",
    "LogicalLibrary",
    "LogicalLibraryState",
    "ArchiveRoute",
    "ArchiveRouteType",
    "ActivityMountRule",
    "GroupMountRule",
    "UserMountRule",
    "MountPolicy",
    "MountType",
    "RepackRequest",
    "RepackState",
    "RepackStatistics",
    "RepackStartInfo",
    "FailedRequest",
    "FailedRequestSummary",
    "UserRequest",
    "UserRequestSummary",
]
