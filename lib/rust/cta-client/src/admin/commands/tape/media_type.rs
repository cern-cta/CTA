// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape media type commands

use crate::typed_admin_cmd;

/// Adds a media type (`mediatype add`).
#[typed_admin_cmd(Mediatype::Add)]
pub struct Add {
    /// Name of the media type
    #[proto(MediaType)]
    pub name: String,
    /// Cartridge model
    pub cartridge: String,
    /// Capacity of a cartridge, in bytes
    pub capacity: u64,
    /// Primary density code
    pub primary_density_code: Option<u64>,
    /// Secondary density code
    pub secondary_density_code: Option<u64>,
    /// Number of wraps
    pub number_of_wraps: Option<u64>,
    /// Minimum longitudinal position
    pub min_lpos: Option<u64>,
    /// Maximum longitudinal position
    pub max_lpos: Option<u64>,
    /// Comment
    pub comment: String,
}

/// Changes a media type (`mediatype ch`).
#[typed_admin_cmd(Mediatype::Ch)]
pub struct Ch {
    /// Name of the media type
    #[proto(MediaType)]
    pub name: String,
    /// New cartridge model
    pub cartridge: Option<String>,
    /// New primary density code
    pub primary_density_code: Option<u64>,
    /// New secondary density code
    pub secondary_density_code: Option<u64>,
    /// New number of wraps
    pub number_of_wraps: Option<u64>,
    /// New minimum longitudinal position
    pub min_lpos: Option<u64>,
    /// New maximum longitudinal position
    pub max_lpos: Option<u64>,
    /// New comment
    pub comment: Option<String>,
}

/// Removes a media type (`mediatype rm`).
#[typed_admin_cmd(Mediatype::Rm)]
pub struct Rm {
    /// Name of the media type
    #[proto(MediaType)]
    pub name: String,
}

/// Lists the media types (`mediatype ls`).
#[typed_admin_cmd(Mediatype::Ls, response = MediaTypeLsItem)]
#[derive(Default)]
pub struct Ls {}
