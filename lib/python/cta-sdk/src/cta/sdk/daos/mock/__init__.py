# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Mock implementations of Data Access Objects for CTA models.

This module provides mock implementations for accessing and manipulating
various CTA entities stored as local JSON files. It is only for testing purposes and you need to install
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
    [CtaMockHandler.create][cta.sdk.daos.mock._handler.CtaMockHandler.create].
"""

from .activity_mount_rule import ActivityMountRuleDaoMock
from .admin_user import AdminUserDaoMock
from .archive_route import ArchiveRouteDaoMock
from .disk_instance import DiskInstanceDaoMock
from .disk_instance_space import DiskInstanceSpaceDaoMock
from .disk_system import DiskSystemDaoMock
from .drive import DriveDaoMock
from .failed_request import FailedRequestDaoMock
from .group_mount_rule import GroupMountRuleDaoMock
from .logical_library import LogicalLibraryDaoMock
from .media_type import MediaTypeDaoMock
from .mount_policy import MountPolicyDaoMock
from .physical_library import PhysicalLibraryDaoMock
from .recycle_bin_tape_file import RecycleBinTapeFileDaoMock
from .repack_request import RepackRequestDaoMock
from .storage_class import StorageClassDaoMock
from .tape import TapeDaoMock
from .tape_file import TapeFileDaoMock
from .tape_pool import TapePoolDaoMock
from .user_mount_rule import UserMountRuleDaoMock
from .user_request import UserRequestDaoMock
from .virtual_organisation import VirtualOrganisationDaoMock

__all__ = [
    "ActivityMountRuleDaoMock",
    "AdminUserDaoMock",
    "ArchiveRouteDaoMock",
    "DiskInstanceDaoMock",
    "DiskInstanceSpaceDaoMock",
    "DiskSystemDaoMock",
    "DriveDaoMock",
    "FailedRequestDaoMock",
    "GroupMountRuleDaoMock",
    "LogicalLibraryDaoMock",
    "MediaTypeDaoMock",
    "MountPolicyDaoMock",
    "PhysicalLibraryDaoMock",
    "RecycleBinTapeFileDaoMock",
    "RepackRequestDaoMock",
    "StorageClassDaoMock",
    "TapeDaoMock",
    "TapeFileDaoMock",
    "TapePoolDaoMock",
    "UserMountRuleDaoMock",
    "UserRequestDaoMock",
    "VirtualOrganisationDaoMock",
]
