// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape media type commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a media type (`mediatype add`).
    #[proto(Mediatype.SubcmdAdd)]
    pub struct Add {
        /// Name of the media type
        name: str => MediaType,
        /// Cartridge model
        cartridge: str => Cartridge,
        /// Capacity of a cartridge, in bytes
        capacity: u64 => Capacity,
        /// Primary density code
        primary_density_code: u64? => PrimaryDensityCode,
        /// Secondary density code
        secondary_density_code: u64? => SecondaryDensityCode,
        /// Number of wraps
        number_of_wraps: u64? => NumberOfWraps,
        /// Minimum longitudinal position
        min_lpos: u64? => MinLpos,
        /// Maximum longitudinal position
        max_lpos: u64? => MaxLpos,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a media type (`mediatype ch`).
    #[proto(Mediatype.SubcmdCh)]
    pub struct Ch {
        /// Name of the media type
        name: str => MediaType,
        /// New cartridge model
        cartridge: str? => Cartridge,
        /// New primary density code
        primary_density_code: u64? => PrimaryDensityCode,
        /// New secondary density code
        secondary_density_code: u64? => SecondaryDensityCode,
        /// New number of wraps
        number_of_wraps: u64? => NumberOfWraps,
        /// New minimum longitudinal position
        min_lpos: u64? => MinLpos,
        /// New maximum longitudinal position
        max_lpos: u64? => MaxLpos,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a media type (`mediatype rm`).
    #[proto(Mediatype.SubcmdRm)]
    pub struct Rm {
        /// Name of the media type
        name: str => MediaType,
    }
}

define_typed_admin_cmd! {
    /// Lists the media types (`mediatype ls`).
    #[proto(Mediatype.SubcmdLs)]
    #[derive(Default)]
    pub struct Ls {}
}
