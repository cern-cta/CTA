# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Data Access Object protocol definitions for CTA models.

This module provides protocol interfaces for accessing and manipulating various
[CTA models][cta.sdk.models] including users, systems, tapes, drives, and policies.
"""

from .activity_mount_rule import ActivityMountRuleDao
from .admin_user import AdminUserDao
from .archive_route import ArchiveRouteDao
from .disk_instance import DiskInstanceDao
from .disk_instance_space import DiskInstanceSpaceDao
from .disk_system import DiskSystemDao
from .drive import DriveDao
from .failed_request import FailedRequestDao
from .group_mount_rule import GroupMountRuleDao
from .logical_library import LogicalLibraryDao
from .media_type import MediaTypeDao
from .mount_policy import MountPolicyDao
from .physical_library import PhysicalLibraryDao
from .recycle_bin_tape_file import RecycleBinTapeFileDao
from .repack_request import RepackRequestDao
from .storage_class import StorageClassDao
from .tape import TapeDao
from .tape_file import TapeFileDao
from .tape_pool import TapePoolDao
from .user_mount_rule import UserMountRuleDao
from .user_request import UserRequestDao
from .virtual_organisation import VirtualOrganisationDao

__all__ = [
    "ActivityMountRuleDao",
    "AdminUserDao",
    "ArchiveRouteDao",
    "DiskInstanceDao",
    "DiskInstanceSpaceDao",
    "DiskSystemDao",
    "DriveDao",
    "FailedRequestDao",
    "GroupMountRuleDao",
    "LogicalLibraryDao",
    "MediaTypeDao",
    "MountPolicyDao",
    "PhysicalLibraryDao",
    "RecycleBinTapeFileDao",
    "RepackRequestDao",
    "StorageClassDao",
    "TapeDao",
    "TapeFileDao",
    "TapePoolDao",
    "UserMountRuleDao",
    "UserRequestDao",
    "VirtualOrganisationDao",
]
