# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.client.protocol import CtaClient
from cta.sdk.daos.mock import (
    ActivityMountRuleDaoMock,
    AdminUserDaoMock,
    ArchiveRouteDaoMock,
    DiskInstanceDaoMock,
    DiskInstanceSpaceDaoMock,
    DiskSystemDaoMock,
    DriveDaoMock,
    FailedRequestDaoMock,
    GroupMountRuleDaoMock,
    LogicalLibraryDaoMock,
    MediaTypeDaoMock,
    MountPolicyDaoMock,
    PhysicalLibraryDaoMock,
    RecycleBinTapeFileDaoMock,
    RepackRequestDaoMock,
    StorageClassDaoMock,
    TapeDaoMock,
    TapeFileDaoMock,
    TapePoolDaoMock,
    UserMountRuleDaoMock,
    UserRequestDaoMock,
    VirtualOrganisationDaoMock,
)
from cta.sdk.daos.mock._handler import CtaMockHandler
from cta.sdk.models import CtaCliVersion, CtaServerVersion, CtaVersion
from typing_extensions import override


class CtaClientMock(CtaClient):
    def __init__(
        self,
        root_dir: str,
    ) -> None:
        """Initialise the MOCK backend client.

        Args:
            root_dir:  Path to the root dir where CTA resources will be stored
        """
        self._mock_handler = CtaMockHandler(root_dir)

    @property
    @override
    def cta_version(self) -> CtaVersion:
        return CtaVersion(
            cli_version=CtaCliVersion(
                "UNSET",
                "UNSET",
            ),
            server_version=CtaServerVersion(
                "UNSET",
                "UNSET",
            ),
            catalogue_connection_string="UNSET",
            catalogue_version="UNSET",
            is_upgrading=False,
            scheduler_backend_name="UNSET",
            instance_name=f"Local Directory({self._mock_handler.root_dir.absolute()})",
        )

    @property
    @override
    def activity_mount_rule(self) -> ActivityMountRuleDaoMock:
        if "_activity_mount_rule" not in self.__dict__:
            self._activity_mount_rule = ActivityMountRuleDaoMock(self._mock_handler)
        return self._activity_mount_rule

    @property
    @override
    def admin_user(self) -> AdminUserDaoMock:
        if "_admin_user" not in self.__dict__:
            self._admin_user = AdminUserDaoMock(self._mock_handler)
        return self._admin_user

    @property
    @override
    def archive_route(self) -> ArchiveRouteDaoMock:
        if "_archive_route" not in self.__dict__:
            self._archive_route = ArchiveRouteDaoMock(self._mock_handler)
        return self._archive_route

    @property
    @override
    def disk_instance_space(self) -> DiskInstanceSpaceDaoMock:
        if "_disk_instance_space" not in self.__dict__:
            self._disk_instance_space = DiskInstanceSpaceDaoMock(self._mock_handler)
        return self._disk_instance_space

    @property
    @override
    def disk_instance(self) -> DiskInstanceDaoMock:
        if "_disk_instance" not in self.__dict__:
            self._disk_instance = DiskInstanceDaoMock(self._mock_handler)
        return self._disk_instance

    @property
    @override
    def disk_system(self) -> DiskSystemDaoMock:
        if "_disk_system" not in self.__dict__:
            self._disk_system = DiskSystemDaoMock(self._mock_handler)
        return self._disk_system

    @property
    @override
    def drive(self) -> DriveDaoMock:
        if "_drive" not in self.__dict__:
            self._drive = DriveDaoMock(self._mock_handler)
        return self._drive

    @property
    @override
    def group_mount_rule(self) -> GroupMountRuleDaoMock:
        if "_group_mount_rule" not in self.__dict__:
            self._group_mount_rule = GroupMountRuleDaoMock(self._mock_handler)
        return self._group_mount_rule

    @property
    @override
    def logical_library(self) -> LogicalLibraryDaoMock:
        if "_logical_library" not in self.__dict__:
            self._logical_library = LogicalLibraryDaoMock(self._mock_handler)
        return self._logical_library

    @property
    @override
    def media_type(self) -> MediaTypeDaoMock:
        if "_media_type" not in self.__dict__:
            self._media_type = MediaTypeDaoMock(self._mock_handler)
        return self._media_type

    @property
    @override
    def mount_policy(self) -> MountPolicyDaoMock:
        if "_mount_policy" not in self.__dict__:
            self._mount_policy = MountPolicyDaoMock(self._mock_handler)
        return self._mount_policy

    @property
    @override
    def physical_library(self) -> PhysicalLibraryDaoMock:
        if "_physical_library" not in self.__dict__:
            self._physical_library = PhysicalLibraryDaoMock(self._mock_handler)
        return self._physical_library

    @property
    @override
    def recycle_bin_tape_file(self) -> RecycleBinTapeFileDaoMock:
        if "_recycle_bin_tape_file" not in self.__dict__:
            self._recycle_bin_tape_file = RecycleBinTapeFileDaoMock(self._mock_handler)
        return self._recycle_bin_tape_file

    @property
    @override
    def user_mount_rule(self) -> UserMountRuleDaoMock:
        if "_user_mount_rule" not in self.__dict__:
            self._user_mount_rule = UserMountRuleDaoMock(self._mock_handler)
        return self._user_mount_rule

    @property
    @override
    def storage_class(self) -> StorageClassDaoMock:
        if "_storage_class" not in self.__dict__:
            self._storage_class = StorageClassDaoMock(self._mock_handler)
        return self._storage_class

    @property
    @override
    def tape_file(self) -> TapeFileDaoMock:
        if "_tape_file" not in self.__dict__:
            self._tape_file = TapeFileDaoMock(self._mock_handler)
        return self._tape_file

    @property
    @override
    def tape_pool(self) -> TapePoolDaoMock:
        if "_tape_pool" not in self.__dict__:
            self._tape_pool = TapePoolDaoMock(self._mock_handler)
        return self._tape_pool

    @property
    @override
    def tape(self) -> TapeDaoMock:
        if "_tape" not in self.__dict__:
            self._tape = TapeDaoMock(self._mock_handler)
        return self._tape

    @property
    @override
    def virtual_organisation(self) -> VirtualOrganisationDaoMock:
        if "_virtual_organisation" not in self.__dict__:
            self._virtual_organisation = VirtualOrganisationDaoMock(self._mock_handler)
        return self._virtual_organisation

    @property
    @override
    def repack_request(self) -> RepackRequestDaoMock:
        if "_repack_request" not in self.__dict__:
            self._repack_request = RepackRequestDaoMock(self._mock_handler)
        return self._repack_request

    @property
    @override
    def failed_request(self) -> FailedRequestDaoMock:
        if "_failed_request" not in self.__dict__:
            self._failed_request = FailedRequestDaoMock(self._mock_handler)
        return self._failed_request

    @property
    @override
    def user_request(self) -> UserRequestDaoMock:
        if "_user_request" not in self.__dict__:
            self._user_request = UserRequestDaoMock(self._mock_handler)
        return self._user_request
