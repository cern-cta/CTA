# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Abstract interface for CTA client implementations.

This module defines the CtaClient Protocol, which specifies the interface that all
CTA client implementations must follow. The design is based on the Abstract Factory
pattern, providing a unified way to access different backend implementations
(e.g., Cli, GRPC) while maintaining a consistent API.

The CtaClient Protocol provides access to 22 different Data Access Objects (Daos),
each responsible for managing a specific type of CTA resource. These Daos provide
CRUD operations and resource-specific actions.

For more details on the Abstract Factory design pattern, see:
https://refactoring.guru/design-patterns/abstract-factory/python/example

Classes:
    CtaClient: Protocol defining the interface for all CTA client implementations
"""

from typing import Protocol

from cta.sdk.daos.protocols import (
    ActivityMountRuleDao,
    AdminUserDao,
    ArchiveRouteDao,
    DiskInstanceDao,
    DiskInstanceSpaceDao,
    DiskSystemDao,
    DriveDao,
    FailedRequestDao,
    GroupMountRuleDao,
    LogicalLibraryDao,
    MediaTypeDao,
    MountPolicyDao,
    PhysicalLibraryDao,
    RecycleBinTapeFileDao,
    RepackRequestDao,
    StorageClassDao,
    TapeDao,
    TapeFileDao,
    TapePoolDao,
    UserMountRuleDao,
    UserRequestDao,
    VirtualOrganisationDao,
)
from cta.sdk.models import CtaVersion


class CtaClient(Protocol):
    """Protocol defining the interface for all CTA client implementations.

    A CtaClient provides access to 22 Data Access Objects (Daos) that manage
    different types of CTA resources. Each Dao property returns an object that
    implements the corresponding Dao protocol, providing methods to create, read,
    update, and delete resources of that type.

    The CtaClient itself is designed as a Protocol rather than a concrete base class
    to allow flexible implementation across different backends (Cli, GRPC, etc.)
    while maintaining a consistent, type-safe interface.

    Example:
        ```python
        from cta.sdk import get_cta_client
        client = get_cta_client("CLI")
        tape = client.tape.get("L12345")
        drives = client.drive.list()
        ```

    See Also:
        [get_cta_client][cta.sdk.factory.get_cta_client]: Factory function to create client instances
        CtaClientCli: Cli backend implementation
    """

    @property
    def cta_version(self) -> CtaVersion:
        """Get CTA version information.

        Returns the version details of the connected CTA system, including
        major, minor, patch versions and other system information.

        Returns:
            CtaVersion: CtaVersion (backend and client configuration information)
        """
        ...

    @property
    def activity_mount_rule(self) -> ActivityMountRuleDao:
        """Activity-based mount rule manager.

        Provides access to mount rules that are triggered based on
        file access patterns or activity types.

        Returns:
            ActivityMountRuleDao: Manager for activity mount rules
        """
        ...

    @property
    def admin_user(self) -> AdminUserDao:
        """Administrator user manager.

        Provides access to manage administrator user accounts and
        their associated privileges.

        Returns:
            AdminUserDao: Manager for admin users
        """
        ...

    @property
    def archive_route(self) -> ArchiveRouteDao:
        """Archive route manager.

        Provides access to manage the mapping between storage classes,
        virtual organisations, and tape pools.

        Returns:
            ArchiveRouteDao: Manager for archive routes
        """
        ...

    @property
    def disk_instance_space(self) -> DiskInstanceSpaceDao:
        """Disk instance space manager.

        Provides access to manage storage spaces within disk instances
        where files are staged before archival to tape.

        Returns:
            DiskInstanceSpaceDao: Manager for disk instance spaces
        """
        ...

    @property
    def disk_instance(self) -> DiskInstanceDao:
        """Disk instance manager.

        Provides access to manage disk instances that serve as staging areas
        for files being archived to or retrieved from tape.

        Returns:
            DiskInstanceDao: Manager for disk instances
        """
        ...

    @property
    def disk_system(self) -> DiskSystemDao:
        """Disk system manager.

        Provides access to manage disk systems and their configuration
        parameters for staging operations.

        Returns:
            DiskSystemDao: Manager for disk systems
        """
        ...

    @property
    def drive(self) -> DriveDao:
        """Tape drive manager.

        Provides access to manage physical tape drives, including their
        state, configuration, and status operations.

        Returns:
            DriveDao: Manager for tape drives
        """
        ...

    @property
    def group_mount_rule(self) -> GroupMountRuleDao:
        """Group-based mount rule manager.

        Provides access to mount rules that are triggered based on
        user group membership.

        Returns:
            GroupMountRuleDao: Manager for group mount rules
        """
        ...

    @property
    def logical_library(self) -> LogicalLibraryDao:
        """Logical library manager.

        Provides access to manage logical libraries, which are logical
        groupings of tape drives and storage.

        Returns:
            LogicalLibraryDao: Manager for logical libraries
        """
        ...

    @property
    def media_type(self) -> MediaTypeDao:
        """Media type manager.

        Provides access to manage tape media types (e.g., LTO9, LTO8)
        and their properties.

        Returns:
            MediaTypeDao: Manager for media types
        """
        ...

    @property
    def mount_policy(self) -> MountPolicyDao:
        """Mount policy manager.

        Provides access to manage mount policies that define priorities
        and scheduling for tape mount operations.

        Returns:
            MountPolicyDao: Manager for mount policies
        """
        ...

    @property
    def physical_library(self) -> PhysicalLibraryDao:
        """Physical library manager.

        Provides access to manage physical tape libraries and their
        hardware configuration.

        Returns:
            PhysicalLibraryDao: Manager for physical libraries
        """
        ...

    @property
    def recycle_bin_tape_file(self) -> RecycleBinTapeFileDao:
        """Recycle bin tape file manager.

        Provides access to query and manage files in the recycle bin
        that have been deleted from tape.

        Returns:
            RecycleBinTapeFileDao: Manager for recycle bin tape files
        """
        ...

    @property
    def user_mount_rule(self) -> UserMountRuleDao:
        """User-based mount rule manager.

        Provides access to mount rules that are triggered based on
        specific user identities.

        Returns:
            UserMountRuleDao: Manager for user mount rules
        """
        ...

    @property
    def storage_class(self) -> StorageClassDao:
        """Storage class manager.

        Provides access to manage storage classes that define how files
        should be archived (replication factor, media type, etc.).

        Returns:
            StorageClassDao: Manager for storage classes
        """
        ...

    @property
    def tape_file(self) -> TapeFileDao:
        """Tape file manager.

        Provides access to query files stored on tape, using various
        lookup methods and filters.

        Returns:
            TapeFileDao: Manager for tape files
        """
        ...

    @property
    def tape_pool(self) -> TapePoolDao:
        """Tape pool manager.

        Provides access to manage tape pools, which are logical groupings
        of tapes for organizing storage resources.

        Returns:
            TapePoolDao: Manager for tape pools
        """
        ...

    @property
    def tape(self) -> TapeDao:
        """Tape manager.

        Provides access to manage physical tapes, including CRUD operations,
        state management, and tape-specific actions.

        Returns:
            TapeDao: Manager for tapes
        """
        ...

    @property
    def virtual_organisation(self) -> VirtualOrganisationDao:
        """Virtual organisation manager.

        Provides access to manage virtual organisations, which represent
        logical groupings of users and resources for quota and billing.

        Returns:
            VirtualOrganisationDao: Manager for virtual organisations
        """
        ...

    @property
    def repack_request(self) -> RepackRequestDao:
        """Tape repack request manager.

        Provides access to manage tape repack operations, which consolidate
        data on partially-full tapes.

        Returns:
            RepackRequestDao: Manager for repack requests
        """
        ...

    @property
    def user_request(self) -> UserRequestDao:
        """Archive/retrieve request manager.

        Provides access to query and manage archive and retrieve requests
        submitted by users or applications.

        Returns:
            UserRequestDao: Manager for requests
        """
        ...

    @property
    def failed_request(self) -> FailedRequestDao:
        """Failed request manager.

        Provides access to query and manage requests that have failed during
        processing, with error details and recovery options.

        Returns:
            FailedRequestDao: Manager for failed requests
        """
        ...
