// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archive route commands

use cta_model::routing::ArchiveRouteType;

use crate::typed_admin_cmd;

/// Adds an archive route (`archiveroute add`).
#[typed_admin_cmd(Archiveroute::Add)]
pub struct Add {
    /// Storage class of the route
    #[proto(StorageClass)]
    pub storage_class_name: String,
    /// Copy number of the route
    pub copy_number: u32,
    /// Type of the route
    #[proto(ArchiveRouteType)]
    pub r#type: Option<ArchiveRouteType>,
    /// Tape pool the copies are written to
    #[proto(TapePool)]
    pub tape_pool_name: String,
    /// Comment
    pub comment: String,
}

/// Changes an archive route (`archiveroute ch`).
#[typed_admin_cmd(Archiveroute::Ch)]
pub struct Ch {
    /// Storage class of the route
    #[proto(StorageClass)]
    pub storage_class_name: String,
    /// Copy number of the route
    pub copy_number: u32,
    /// Type of the route
    #[proto(ArchiveRouteType)]
    pub r#type: ArchiveRouteType,
    /// New tape pool
    #[proto(TapePool)]
    pub tape_pool_name: Option<String>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes an archive route (`archiveroute rm`).
#[typed_admin_cmd(Archiveroute::Rm)]
pub struct Rm {
    /// Storage class of the route
    #[proto(StorageClass)]
    pub storage_class_name: String,
    /// Copy number of the route
    pub copy_number: u32,
    /// Type of the route
    #[proto(ArchiveRouteType)]
    pub r#type: ArchiveRouteType,
}

/// Lists the archive routes (`archiveroute ls`).
#[typed_admin_cmd(Archiveroute::Ls, response = ArchiveRouteLsItem)]
#[derive(Default)]
pub struct Ls {}
