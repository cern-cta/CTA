// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archive route commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds an archive route (`archiveroute add`).
    #[proto(Archiveroute.SubcmdAdd)]
    pub struct Add {
        /// Storage class of the route
        storage_class: str => StorageClass,
        /// Copy number of the route
        copy_nb: u64 => CopyNumber,
        /// Type of the route
        archive_route_type: str? => ArchiveRouteType,
        /// Tape pool the copies are written to
        tape_pool: str => TapePool,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes an archive route (`archiveroute ch`).
    #[proto(Archiveroute.SubcmdCh)]
    pub struct Ch {
        /// Storage class of the route
        storage_class: str => StorageClass,
        /// Copy number of the route
        copy_nb: u64 => CopyNumber,
        /// Type of the route
        archive_route_type: str => ArchiveRouteType,
        /// New tape pool
        tape_pool: str? => TapePool,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes an archive route (`archiveroute rm`).
    #[proto(Archiveroute.SubcmdRm)]
    pub struct Rm {
        /// Storage class of the route
        storage_class: str => StorageClass,
        /// Copy number of the route
        copy_nb: u64 => CopyNumber,
        /// Type of the route
        archive_route_type: str => ArchiveRouteType,
    }
}

define_typed_admin_cmd! {
    /// Lists the archive routes (`archiveroute ls`).
    #[proto(Archiveroute.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
