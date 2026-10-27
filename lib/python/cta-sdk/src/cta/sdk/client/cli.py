# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

from cta.sdk.client.protocol import CtaClient
from cta.sdk.daos.cli import (
    ActivityMountRuleDaoCli,
    AdminUserDaoCli,
    ArchiveRouteDaoCli,
    DiskInstanceDaoCli,
    DiskInstanceSpaceDaoCli,
    DiskSystemDaoCli,
    DriveDaoCli,
    FailedRequestDaoCli,
    GroupMountRuleDaoCli,
    LogicalLibraryDaoCli,
    MediaTypeDaoCli,
    MountPolicyDaoCli,
    PhysicalLibraryDaoCli,
    RecycleBinTapeFileDaoCli,
    RepackRequestDaoCli,
    StorageClassDaoCli,
    TapeDaoCli,
    TapeFileDaoCli,
    TapePoolDaoCli,
    UserMountRuleDaoCli,
    UserRequestDaoCli,
    VirtualOrganisationDaoCli,
)
from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.models import CtaCliVersion, CtaServerVersion, CtaVersion


class CtaClientCli(CtaClient):
    def __init__(
        self,
        exec_path: str = "/usr/bin/cta-admin",
        timeout: int = 60,
        sss_keytab_file: Optional[str] = None,
        cta_conf_file: Optional[str] = None,
    ) -> None:
        """Initialise the CLI backend client.

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
        self._cli_wrapper = CtaCliWrapper(
            exec_path=exec_path,
            timeout=timeout,
            sss_keytab_file=sss_keytab_file,
            cta_conf_file=cta_conf_file,
        )

    @property
    def cta_version(self) -> CtaVersion:
        raw_version = self._cli_wrapper.cta_version
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
    def activity_mount_rule(self) -> ActivityMountRuleDaoCli:
        if "_activity_mount_rule" not in self.__dict__:
            self._activity_mount_rule = ActivityMountRuleDaoCli(self._cli_wrapper)
        return self._activity_mount_rule

    @property
    def admin_user(self) -> AdminUserDaoCli:
        if "_admin_user" not in self.__dict__:
            self._admin_user = AdminUserDaoCli(self._cli_wrapper)
        return self._admin_user

    @property
    def archive_route(self) -> ArchiveRouteDaoCli:
        if "_archive_route" not in self.__dict__:
            self._archive_route = ArchiveRouteDaoCli(self._cli_wrapper)
        return self._archive_route

    @property
    def disk_instance_space(self) -> DiskInstanceSpaceDaoCli:
        if "_disk_instance_space" not in self.__dict__:
            self._disk_instance_space = DiskInstanceSpaceDaoCli(self._cli_wrapper)
        return self._disk_instance_space

    @property
    def disk_instance(self) -> DiskInstanceDaoCli:
        if "_disk_instance" not in self.__dict__:
            self._disk_instance = DiskInstanceDaoCli(self._cli_wrapper)
        return self._disk_instance

    @property
    def disk_system(self) -> DiskSystemDaoCli:
        if "_disk_system" not in self.__dict__:
            self._disk_system = DiskSystemDaoCli(self._cli_wrapper)
        return self._disk_system

    @property
    def drive(self) -> DriveDaoCli:
        if "_drive" not in self.__dict__:
            self._drive = DriveDaoCli(self._cli_wrapper)
        return self._drive

    @property
    def group_mount_rule(self) -> GroupMountRuleDaoCli:
        if "_group_mount_rule" not in self.__dict__:
            self._group_mount_rule = GroupMountRuleDaoCli(self._cli_wrapper)
        return self._group_mount_rule

    @property
    def logical_library(self) -> LogicalLibraryDaoCli:
        if "_logical_library" not in self.__dict__:
            self._logical_library = LogicalLibraryDaoCli(self._cli_wrapper)
        return self._logical_library

    @property
    def media_type(self) -> MediaTypeDaoCli:
        if "_media_type" not in self.__dict__:
            self._media_type = MediaTypeDaoCli(self._cli_wrapper)
        return self._media_type

    @property
    def mount_policy(self) -> MountPolicyDaoCli:
        if "_mount_policy" not in self.__dict__:
            self._mount_policy = MountPolicyDaoCli(self._cli_wrapper)
        return self._mount_policy

    @property
    def physical_library(self) -> PhysicalLibraryDaoCli:
        if "_physical_library" not in self.__dict__:
            self._physical_library = PhysicalLibraryDaoCli(self._cli_wrapper)
        return self._physical_library

    @property
    def recycle_bin_tape_file(self) -> RecycleBinTapeFileDaoCli:
        if "_recycle_bin_tape_file" not in self.__dict__:
            self._recycle_bin_tape_file = RecycleBinTapeFileDaoCli(self._cli_wrapper)
        return self._recycle_bin_tape_file

    @property
    def user_mount_rule(self) -> UserMountRuleDaoCli:
        if "_user_mount_rule" not in self.__dict__:
            self._user_mount_rule = UserMountRuleDaoCli(self._cli_wrapper)
        return self._user_mount_rule

    @property
    def storage_class(self) -> StorageClassDaoCli:
        if "_storage_class" not in self.__dict__:
            self._storage_class = StorageClassDaoCli(self._cli_wrapper)
        return self._storage_class

    @property
    def tape_file(self) -> TapeFileDaoCli:
        if "_tape_file" not in self.__dict__:
            self._tape_file = TapeFileDaoCli(self._cli_wrapper)
        return self._tape_file

    @property
    def tape_pool(self) -> TapePoolDaoCli:
        if "_tape_pool" not in self.__dict__:
            self._tape_pool = TapePoolDaoCli(self._cli_wrapper)
        return self._tape_pool

    @property
    def tape(self) -> TapeDaoCli:
        if "_tape" not in self.__dict__:
            self._tape = TapeDaoCli(self._cli_wrapper)
        return self._tape

    @property
    def virtual_organisation(self) -> VirtualOrganisationDaoCli:
        if "_virtual_organisation" not in self.__dict__:
            self._virtual_organisation = VirtualOrganisationDaoCli(self._cli_wrapper)
        return self._virtual_organisation

    @property
    def repack_request(self) -> RepackRequestDaoCli:
        if "_repack_request" not in self.__dict__:
            self._repack_request = RepackRequestDaoCli(self._cli_wrapper)
        return self._repack_request

    @property
    def failed_request(self) -> FailedRequestDaoCli:
        if "_failed_request" not in self.__dict__:
            self._failed_request = FailedRequestDaoCli(self._cli_wrapper)
        return self._failed_request

    @property
    def user_request(self) -> UserRequestDaoCli:
        if "_user_request" not in self.__dict__:
            self._user_request = UserRequestDaoCli(self._cli_wrapper)
        return self._user_request
