# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""File interface implementations of Data Access Objects for CTA models.

This module provides File-based implementations for accessing and manipulating
various CTA entities through File-based interfaces. It is only for testing purposes and you need to install
cta-sdk with dev extra `cta-sdk[dev]` .

You can have a root directory with the following structure:

Example:

    root_dir/
    ├── adminuser/
    │   ├── user1.json
    │   └── user2.json
    ├── archiveroute/
    │   ├── sc1__1__default.json
    │   └── sc1__2__repack.json
    ├── diskinstance/
    │   ├── instance1.json
    │   └── instance2.json
    └── ...

Each resource is stored as one JSON file, named after its primary key(s) joined by `__`,
in a directory named after its model.

Note:
    Resources that CTA itself produces (drives, tape files, failed requests and user requests)
    have no `create` in their Dao. Seed them by building the model and passing it to
    [CtaFileHandler.create][cta.sdk.daos.file._handler.CtaFileHandler.create].
"""

from .activity_mount_rule import ActivityMountRuleDaoFile
from .admin_user import AdminUserDaoFile
from .archive_route import ArchiveRouteDaoFile
from .disk_instance import DiskInstanceDaoFile
from .disk_instance_space import DiskInstanceSpaceDaoFile
from .disk_system import DiskSystemDaoFile
from .drive import DriveDaoFile
from .failed_request import FailedRequestDaoFile
from .group_mount_rule import GroupMountRuleDaoFile
from .logical_library import LogicalLibraryDaoFile
from .media_type import MediaTypeDaoFile
from .mount_policy import MountPolicyDaoFile
from .physical_library import PhysicalLibraryDaoFile
from .recycle_bin_tape_file import RecycleBinTapeFileDaoFile
from .repack_request import RepackRequestDaoFile
from .storage_class import StorageClassDaoFile
from .tape import TapeDaoFile
from .tape_file import TapeFileDaoFile
from .tape_pool import TapePoolDaoFile
from .user_mount_rule import UserMountRuleDaoFile
from .user_request import UserRequestDaoFile
from .virtual_organisation import VirtualOrganisationDaoFile

__all__ = [
    "AdminUserDaoFile",
    "VirtualOrganisationDaoFile",
    "DiskInstanceDaoFile",
    "DiskInstanceSpaceDaoFile",
    "DiskSystemDaoFile",
    "TapeFileDaoFile",
    "RecycleBinTapeFileDaoFile",
    "StorageClassDaoFile",
    "DriveDaoFile",
    "MediaTypeDaoFile",
    "PhysicalLibraryDaoFile",
    "TapeDaoFile",
    "TapePoolDaoFile",
    "LogicalLibraryDaoFile",
    "ArchiveRouteDaoFile",
    "ActivityMountRuleDaoFile",
    "GroupMountRuleDaoFile",
    "UserMountRuleDaoFile",
    "MountPolicyDaoFile",
    "RepackRequestDaoFile",
    "FailedRequestDaoFile",
    "UserRequestDaoFile",
]
