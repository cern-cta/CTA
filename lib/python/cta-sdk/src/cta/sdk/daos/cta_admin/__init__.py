# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""cta-admin implementations of Data Access Objects for CTA models.

This module provides cta-admin-based implementations for accessing and manipulating
various CTA entities by running the `cta-admin` command.
"""

from .activity_mount_rule import ActivityMountRuleDaoCtaAdmin
from .admin_user import AdminUserDaoCtaAdmin
from .archive_route import ArchiveRouteDaoCtaAdmin
from .disk_instance import DiskInstanceDaoCtaAdmin
from .disk_instance_space import DiskInstanceSpaceDaoCtaAdmin
from .disk_system import DiskSystemDaoCtaAdmin
from .drive import DriveDaoCtaAdmin
from .failed_request import FailedRequestDaoCtaAdmin
from .group_mount_rule import GroupMountRuleDaoCtaAdmin
from .logical_library import LogicalLibraryDaoCtaAdmin
from .media_type import MediaTypeDaoCtaAdmin
from .mount_policy import MountPolicyDaoCtaAdmin
from .physical_library import PhysicalLibraryDaoCtaAdmin
from .recycle_bin_tape_file import RecycleBinTapeFileDaoCtaAdmin
from .repack_request import RepackRequestDaoCtaAdmin
from .storage_class import StorageClassDaoCtaAdmin
from .tape import TapeDaoCtaAdmin
from .tape_file import TapeFileDaoCtaAdmin
from .tape_pool import TapePoolDaoCtaAdmin
from .user_mount_rule import UserMountRuleDaoCtaAdmin
from .user_request import UserRequestDaoCtaAdmin
from .virtual_organisation import VirtualOrganisationDaoCtaAdmin

__all__ = [
    "ActivityMountRuleDaoCtaAdmin",
    "AdminUserDaoCtaAdmin",
    "ArchiveRouteDaoCtaAdmin",
    "DiskInstanceDaoCtaAdmin",
    "DiskInstanceSpaceDaoCtaAdmin",
    "DiskSystemDaoCtaAdmin",
    "DriveDaoCtaAdmin",
    "FailedRequestDaoCtaAdmin",
    "GroupMountRuleDaoCtaAdmin",
    "LogicalLibraryDaoCtaAdmin",
    "MediaTypeDaoCtaAdmin",
    "MountPolicyDaoCtaAdmin",
    "PhysicalLibraryDaoCtaAdmin",
    "RecycleBinTapeFileDaoCtaAdmin",
    "RepackRequestDaoCtaAdmin",
    "StorageClassDaoCtaAdmin",
    "TapeDaoCtaAdmin",
    "TapeFileDaoCtaAdmin",
    "TapePoolDaoCtaAdmin",
    "UserMountRuleDaoCtaAdmin",
    "UserRequestDaoCtaAdmin",
    "VirtualOrganisationDaoCtaAdmin",
]
