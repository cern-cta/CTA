// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archive route commands

use cta_model::routing::ArchiveRouteType;

use crate::typed_admin_cmd;

/// Adds an archive route (`archiveroute add`).
#[typed_admin_cmd(Archiveroute::Add)]
pub struct Add {
    /// Storage class of the route
    pub storage_class: String,
    /// Copy number of the route
    #[proto(CopyNumber)]
    pub copy_nb: u64,
    /// Type of the route
    pub archive_route_type: Option<ArchiveRouteType>,
    /// Tape pool the copies are written to
    pub tape_pool: String,
    /// Comment
    pub comment: String,
}

/// Changes an archive route (`archiveroute ch`).
#[typed_admin_cmd(Archiveroute::Ch)]
pub struct Ch {
    /// Storage class of the route
    pub storage_class: String,
    /// Copy number of the route
    #[proto(CopyNumber)]
    pub copy_nb: u64,
    /// Type of the route
    pub archive_route_type: ArchiveRouteType,
    /// New tape pool
    pub tape_pool: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes an archive route (`archiveroute rm`).
#[typed_admin_cmd(Archiveroute::Rm)]
pub struct Rm {
    /// Storage class of the route
    pub storage_class: String,
    /// Copy number of the route
    #[proto(CopyNumber)]
    pub copy_nb: u64,
    /// Type of the route
    pub archive_route_type: ArchiveRouteType,
}

/// Lists the archive routes (`archiveroute ls`).
#[typed_admin_cmd(Archiveroute::Ls, response = ArchiveRouteLsItem)]
#[derive(Default)]
pub struct Ls {}
