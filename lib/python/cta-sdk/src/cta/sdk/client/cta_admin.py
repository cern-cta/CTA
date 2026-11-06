# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

from cta.sdk.client.protocol import CtaClient
from cta.sdk.daos.cta_admin import (
    ActivityMountRuleDaoCtaAdmin,
    AdminUserDaoCtaAdmin,
    ArchiveRouteDaoCtaAdmin,
    DiskInstanceDaoCtaAdmin,
    DiskInstanceSpaceDaoCtaAdmin,
    DiskSystemDaoCtaAdmin,
    DriveDaoCtaAdmin,
    FailedRequestDaoCtaAdmin,
    GroupMountRuleDaoCtaAdmin,
    LogicalLibraryDaoCtaAdmin,
    MediaTypeDaoCtaAdmin,
    MountPolicyDaoCtaAdmin,
    PhysicalLibraryDaoCtaAdmin,
    RecycleBinTapeFileDaoCtaAdmin,
    RepackRequestDaoCtaAdmin,
    StorageClassDaoCtaAdmin,
    TapeDaoCtaAdmin,
    TapeFileDaoCtaAdmin,
    TapePoolDaoCtaAdmin,
    UserMountRuleDaoCtaAdmin,
    UserRequestDaoCtaAdmin,
    VirtualOrganisationDaoCtaAdmin,
)
from cta.sdk.daos.cta_admin._wrapper import CtaAdminWrapper
from cta.sdk.models import CtaCliVersion, CtaServerVersion, CtaVersion
from typing_extensions import override


class CtaClientCtaAdmin(CtaClient):
    def __init__(
        self,
        exec_path: str = "/usr/bin/cta-admin",
        timeout: int = 60,
        sss_keytab_file: Optional[str] = None,
        cta_conf_file: Optional[str] = None,
    ) -> None:
        """Initialise the CTA_ADMIN backend client.

        Args:
            exec_path: Path to the cta-admin executable
                (default: ``/usr/bin/cta-admin``).
            timeout: Command execution timeout in seconds (default: ``60``).
            sss_keytab_file: Path to the Kerberos SSS keytab file used for
                authentication (default: ``/etc/cta/tape-local.sss.keytab``).
                Overridden by the ``CTAXrdSecSSSKT`` environment variable if
                set and readable.
            cta_conf_file: Optional path to a CTA configuration file passed
                to every cta-admin invocation (default: ``None``).
        """
        self._cta_admin_wrapper = CtaAdminWrapper(
            exec_path=exec_path,
            timeout=timeout,
            sss_keytab_file=sss_keytab_file,
            cta_conf_file=cta_conf_file,
        )

    @property
    @override
    def cta_version(self) -> CtaVersion:
        raw_version = self._cta_admin_wrapper.cta_version
        return CtaVersion(
            cli_version=CtaCliVersion(
                raw_version["clientVersion"]["ctaVersion"],
                raw_version["clientVersion"].get("xrootdSsiProtobufInterfaceVersion"),
            ),
            server_version=CtaServerVersion(
                raw_version["serverVersion"]["ctaVersion"],
                raw_version["serverVersion"].get("xrootdSsiProtobufInterfaceVersion"),
            ),
            catalogue_connection_string=raw_version["catalogueConnectionString"],
            catalogue_version=raw_version["catalogueVersion"],
            is_upgrading=raw_version["isUpgrading"],
            scheduler_backend_name=raw_version["schedulerBackendName"],
            instance_name=raw_version["instanceName"],
        )

    @property
    @override
    def activity_mount_rule(self) -> ActivityMountRuleDaoCtaAdmin:
        if "_activity_mount_rule" not in self.__dict__:
            self._activity_mount_rule = ActivityMountRuleDaoCtaAdmin(self._cta_admin_wrapper)
        return self._activity_mount_rule

    @property
    @override
    def admin_user(self) -> AdminUserDaoCtaAdmin:
        if "_admin_user" not in self.__dict__:
            self._admin_user = AdminUserDaoCtaAdmin(self._cta_admin_wrapper)
        return self._admin_user

    @property
    @override
    def archive_route(self) -> ArchiveRouteDaoCtaAdmin:
        if "_archive_route" not in self.__dict__:
            self._archive_route = ArchiveRouteDaoCtaAdmin(self._cta_admin_wrapper)
        return self._archive_route

    @property
    @override
    def disk_instance_space(self) -> DiskInstanceSpaceDaoCtaAdmin:
        if "_disk_instance_space" not in self.__dict__:
            self._disk_instance_space = DiskInstanceSpaceDaoCtaAdmin(self._cta_admin_wrapper)
        return self._disk_instance_space

    @property
    @override
    def disk_instance(self) -> DiskInstanceDaoCtaAdmin:
        if "_disk_instance" not in self.__dict__:
            self._disk_instance = DiskInstanceDaoCtaAdmin(self._cta_admin_wrapper)
        return self._disk_instance

    @property
    @override
    def disk_system(self) -> DiskSystemDaoCtaAdmin:
        if "_disk_system" not in self.__dict__:
            self._disk_system = DiskSystemDaoCtaAdmin(self._cta_admin_wrapper)
        return self._disk_system

    @property
    @override
    def drive(self) -> DriveDaoCtaAdmin:
        if "_drive" not in self.__dict__:
            self._drive = DriveDaoCtaAdmin(self._cta_admin_wrapper)
        return self._drive

    @property
    @override
    def group_mount_rule(self) -> GroupMountRuleDaoCtaAdmin:
        if "_group_mount_rule" not in self.__dict__:
            self._group_mount_rule = GroupMountRuleDaoCtaAdmin(self._cta_admin_wrapper)
        return self._group_mount_rule

    @property
    @override
    def logical_library(self) -> LogicalLibraryDaoCtaAdmin:
        if "_logical_library" not in self.__dict__:
            self._logical_library = LogicalLibraryDaoCtaAdmin(self._cta_admin_wrapper)
        return self._logical_library

    @property
    @override
    def media_type(self) -> MediaTypeDaoCtaAdmin:
        if "_media_type" not in self.__dict__:
            self._media_type = MediaTypeDaoCtaAdmin(self._cta_admin_wrapper)
        return self._media_type

    @property
    @override
    def mount_policy(self) -> MountPolicyDaoCtaAdmin:
        if "_mount_policy" not in self.__dict__:
            self._mount_policy = MountPolicyDaoCtaAdmin(self._cta_admin_wrapper)
        return self._mount_policy

    @property
    @override
    def physical_library(self) -> PhysicalLibraryDaoCtaAdmin:
        if "_physical_library" not in self.__dict__:
            self._physical_library = PhysicalLibraryDaoCtaAdmin(self._cta_admin_wrapper)
        return self._physical_library

    @property
    @override
    def recycle_bin_tape_file(self) -> RecycleBinTapeFileDaoCtaAdmin:
        if "_recycle_bin_tape_file" not in self.__dict__:
            self._recycle_bin_tape_file = RecycleBinTapeFileDaoCtaAdmin(self._cta_admin_wrapper)
        return self._recycle_bin_tape_file

    @property
    @override
    def user_mount_rule(self) -> UserMountRuleDaoCtaAdmin:
        if "_user_mount_rule" not in self.__dict__:
            self._user_mount_rule = UserMountRuleDaoCtaAdmin(self._cta_admin_wrapper)
        return self._user_mount_rule

    @property
    @override
    def storage_class(self) -> StorageClassDaoCtaAdmin:
        if "_storage_class" not in self.__dict__:
            self._storage_class = StorageClassDaoCtaAdmin(self._cta_admin_wrapper)
        return self._storage_class

    @property
    @override
    def tape_file(self) -> TapeFileDaoCtaAdmin:
        if "_tape_file" not in self.__dict__:
            self._tape_file = TapeFileDaoCtaAdmin(self._cta_admin_wrapper)
        return self._tape_file

    @property
    @override
    def tape_pool(self) -> TapePoolDaoCtaAdmin:
        if "_tape_pool" not in self.__dict__:
            self._tape_pool = TapePoolDaoCtaAdmin(self._cta_admin_wrapper)
        return self._tape_pool

    @property
    @override
    def tape(self) -> TapeDaoCtaAdmin:
        if "_tape" not in self.__dict__:
            self._tape = TapeDaoCtaAdmin(self._cta_admin_wrapper)
        return self._tape

    @property
    @override
    def virtual_organisation(self) -> VirtualOrganisationDaoCtaAdmin:
        if "_virtual_organisation" not in self.__dict__:
            self._virtual_organisation = VirtualOrganisationDaoCtaAdmin(self._cta_admin_wrapper)
        return self._virtual_organisation

    @property
    @override
    def repack_request(self) -> RepackRequestDaoCtaAdmin:
        if "_repack_request" not in self.__dict__:
            self._repack_request = RepackRequestDaoCtaAdmin(self._cta_admin_wrapper)
        return self._repack_request

    @property
    @override
    def failed_request(self) -> FailedRequestDaoCtaAdmin:
        if "_failed_request" not in self.__dict__:
            self._failed_request = FailedRequestDaoCtaAdmin(self._cta_admin_wrapper)
        return self._failed_request

    @property
    @override
    def user_request(self) -> UserRequestDaoCtaAdmin:
        if "_user_request" not in self.__dict__:
            self._user_request = UserRequestDaoCtaAdmin(self._cta_admin_wrapper)
        return self._user_request
