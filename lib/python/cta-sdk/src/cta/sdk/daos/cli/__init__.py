# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Command-line interface implementations of Data Access Objects for CTA models.

This module provides CLI-based implementations for accessing and manipulating
various CTA entities through command-line interfaces.
"""

from .activity_mount_rule import ActivityMountRuleDaoCli
from .admin_user import AdminUserDaoCli
from .archive_route import ArchiveRouteDaoCli
from .disk_instance import DiskInstanceDaoCli
from .disk_instance_space import DiskInstanceSpaceDaoCli
from .disk_system import DiskSystemDaoCli
from .drive import DriveDaoCli
from .failed_request import FailedRequestDaoCli
from .group_mount_rule import GroupMountRuleDaoCli
from .logical_library import LogicalLibraryDaoCli
from .media_type import MediaTypeDaoCli
from .mount_policy import MountPolicyDaoCli
from .physical_library import PhysicalLibraryDaoCli
from .recycle_bin_tape_file import RecycleBinTapeFileDaoCli
from .repack_request import RepackRequestDaoCli
from .storage_class import StorageClassDaoCli
from .tape import TapeDaoCli
from .tape_file import TapeFileDaoCli
from .tape_pool import TapePoolDaoCli
from .user_mount_rule import UserMountRuleDaoCli
from .user_request import UserRequestDaoCli
from .virtual_organisation import VirtualOrganisationDaoCli

__all__ = [
    "AdminUserDaoCli",
    "VirtualOrganisationDaoCli",
    "DiskInstanceDaoCli",
    "DiskInstanceSpaceDaoCli",
    "DiskSystemDaoCli",
    "TapeFileDaoCli",
    "RecycleBinTapeFileDaoCli",
    "StorageClassDaoCli",
    "DriveDaoCli",
    "MediaTypeDaoCli",
    "PhysicalLibraryDaoCli",
    "TapeDaoCli",
    "TapePoolDaoCli",
    "LogicalLibraryDaoCli",
    "ArchiveRouteDaoCli",
    "ActivityMountRuleDaoCli",
    "GroupMountRuleDaoCli",
    "UserMountRuleDaoCli",
    "MountPolicyDaoCli",
    "RepackRequestDaoCli",
    "FailedRequestDaoCli",
    "UserRequestDaoCli",
]
