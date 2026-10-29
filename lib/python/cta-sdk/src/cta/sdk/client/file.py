# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from cta.sdk.client.protocol import CtaClient
from cta.sdk.daos.file import (
    ActivityMountRuleDaoFile,
    AdminUserDaoFile,
    ArchiveRouteDaoFile,
    DiskInstanceDaoFile,
    DiskInstanceSpaceDaoFile,
    DriveDaoFile,
    FailedRequestDaoFile,
    GroupMountRuleDaoFile,
    LogicalLibraryDaoFile,
    MediaTypeDaoFile,
    MountPolicyDaoFile,
    PhysicalLibraryDaoFile,
    RecycleBinTapeFileDaoFile,
    RepackRequestDaoFile,
    StorageClassDaoFile,
    TapeDaoFile,
    TapeFileDaoFile,
    TapePoolDaoFile,
    UserMountRuleDaoFile,
    UserRequestDaoFile,
    VirtualOrganisationDaoFile,
)
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.models import CtaCliVersion, CtaServerVersion, CtaVersion


class CtaClientFile(CtaClient):
    def __init__(
        self,
        root_dir: str,
    ) -> None:
        """Initialise the File backend client.

        Args:
            root_dir:  Path to the root dir where CTA resources will be stored
        """
        self._file_handler = CtaFileHandler(root_dir)

    @property
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
            instance_name=f"Local Directory({self._file_handler.root_dir.absolute()})",
        )

    @property
    def activity_mount_rule(self) -> ActivityMountRuleDaoFile:
        if "_activity_mount_rule" not in self.__dict__:
            self._activity_mount_rule = ActivityMountRuleDaoFile(self._file_handler)
        return self._activity_mount_rule

    @property
    def admin_user(self) -> AdminUserDaoFile:
        if "_admin_user" not in self.__dict__:
            self._admin_user = AdminUserDaoFile(self._file_handler)
        return self._admin_user

    @property
    def archive_route(self) -> ArchiveRouteDaoFile:
        if "_archive_route" not in self.__dict__:
            self._archive_route = ArchiveRouteDaoFile(self._file_handler)
        return self._archive_route

    @property
    def disk_instance_space(self) -> DiskInstanceSpaceDaoFile:
        if "_disk_instance_space" not in self.__dict__:
            self._disk_instance_space = DiskInstanceSpaceDaoFile(self._file_handler)
        return self._disk_instance_space

    @property
    def disk_instance(self) -> DiskInstanceDaoFile:
        if "_disk_instance" not in self.__dict__:
            self._disk_instance = DiskInstanceDaoFile(self._file_handler)
        return self._disk_instance

    @property
    def disk_system(self) -> DiskInstanceDaoFile:
        if "_disk_system" not in self.__dict__:
            self._disk_system = DiskInstanceDaoFile(self._file_handler)
        return self._disk_system

    @property
    def drive(self) -> DriveDaoFile:
        if "_drive" not in self.__dict__:
            self._drive = DriveDaoFile(self._file_handler)
        return self._drive

    @property
    def group_mount_rule(self) -> GroupMountRuleDaoFile:
        if "_group_mount_rule" not in self.__dict__:
            self._group_mount_rule = GroupMountRuleDaoFile(self._file_handler)
        return self._group_mount_rule

    @property
    def logical_library(self) -> LogicalLibraryDaoFile:
        if "_logical_library" not in self.__dict__:
            self._logical_library = LogicalLibraryDaoFile(self._file_handler)
        return self._logical_library

    @property
    def media_type(self) -> MediaTypeDaoFile:
        if "_media_type" not in self.__dict__:
            self._media_type = MediaTypeDaoFile(self._file_handler)
        return self._media_type

    @property
    def mount_policy(self) -> MountPolicyDaoFile:
        if "_mount_policy" not in self.__dict__:
            self._mount_policy = MountPolicyDaoFile(self._file_handler)
        return self._mount_policy

    @property
    def physical_library(self) -> PhysicalLibraryDaoFile:
        if "_physical_library" not in self.__dict__:
            self._physical_library = PhysicalLibraryDaoFile(self._file_handler)
        return self._physical_library

    @property
    def recycle_bin_tape_file(self) -> RecycleBinTapeFileDaoFile:
        if "_recycle_bin_tape_file" not in self.__dict__:
            self._recycle_bin_tape_file = RecycleBinTapeFileDaoFile(self._file_handler)
        return self._recycle_bin_tape_file

    @property
    def user_mount_rule(self) -> UserMountRuleDaoFile:
        if "_user_mount_rule" not in self.__dict__:
            self._user_mount_rule = UserMountRuleDaoFile(self._file_handler)
        return self._user_mount_rule

    @property
    def storage_class(self) -> StorageClassDaoFile:
        if "_storage_class" not in self.__dict__:
            self._storage_class = StorageClassDaoFile(self._file_handler)
        return self._storage_class

    @property
    def tape_file(self) -> TapeFileDaoFile:
        if "_tape_file" not in self.__dict__:
            self._tape_file = TapeFileDaoFile(self._file_handler)
        return self._tape_file

    @property
    def tape_pool(self) -> TapePoolDaoFile:
        if "_tape_pool" not in self.__dict__:
            self._tape_pool = TapePoolDaoFile(self._file_handler)
        return self._tape_pool

    @property
    def tape(self) -> TapeDaoFile:
        if "_tape" not in self.__dict__:
            self._tape = TapeDaoFile(self._file_handler)
        return self._tape

    @property
    def virtual_organisation(self) -> VirtualOrganisationDaoFile:
        if "_virtual_organisation" not in self.__dict__:
            self._virtual_organisation = VirtualOrganisationDaoFile(self._file_handler)
        return self._virtual_organisation

    @property
    def repack_request(self) -> RepackRequestDaoFile:
        if "_repack_request" not in self.__dict__:
            self._repack_request = RepackRequestDaoFile(self._file_handler)
        return self._repack_request

    @property
    def failed_request(self) -> FailedRequestDaoFile:
        if "_failed_request" not in self.__dict__:
            self._failed_request = FailedRequestDaoFile(self._file_handler)
        return self._failed_request

    @property
    def user_request(self) -> UserRequestDaoFile:
        if "_user_request" not in self.__dict__:
            self._user_request = UserRequestDaoFile(self._file_handler)
        return self._user_request
