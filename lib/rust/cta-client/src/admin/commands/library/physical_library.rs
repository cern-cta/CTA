// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Physical library commands

use crate::typed_admin_cmd;

/// Adds a physical library (`physicallibrary add`).
#[typed_admin_cmd(Physicallibrary::Add)]
pub struct Add {
    /// Name of the physical library
    #[proto(PhysicalLibrary)]
    pub name: String,
    /// Manufacturer of the library
    pub manufacturer: String,
    /// Model of the library
    #[proto(LibraryModel)]
    pub model: String,
    /// Type of the library
    pub library_type: Option<String>,
    /// URL of the library GUI
    pub gui_url: Option<String>,
    /// URL of the library webcam
    pub webcam_url: Option<String>,
    /// Location of the library
    #[proto(LibraryLocation)]
    pub location: Option<String>,
    /// Number of physical cartridge slots
    pub nb_physical_cartridge_slots: u64,
    /// Number of available cartridge slots
    pub nb_available_cartridge_slots: Option<u64>,
    /// Number of physical drive slots
    pub nb_physical_drive_slots: u64,
    /// Comment
    pub comment: Option<String>,
}

/// Changes a physical library (`physicallibrary ch`).
#[typed_admin_cmd(Physicallibrary::Ch)]
pub struct Ch {
    /// Name of the physical library
    #[proto(PhysicalLibrary)]
    pub name: String,
    /// New model
    #[proto(LibraryModel)]
    pub model: Option<String>,
    /// New type
    pub library_type: Option<String>,
    /// New URL of the library GUI
    pub gui_url: Option<String>,
    /// New URL of the library webcam
    pub webcam_url: Option<String>,
    /// New location
    #[proto(LibraryLocation)]
    pub location: Option<String>,
    /// New number of physical cartridge slots
    pub nb_physical_cartridge_slots: Option<u64>,
    /// New number of available cartridge slots
    pub nb_available_cartridge_slots: Option<u64>,
    /// New number of physical drive slots
    pub nb_physical_drive_slots: Option<u64>,
    /// New comment
    pub comment: Option<String>,
    /// Whether the library is disabled
    pub disabled: Option<bool>,
    /// Why the library is disabled
    pub disabled_reason: Option<String>,
}

/// Removes a physical library (`physicallibrary rm`).
#[typed_admin_cmd(Physicallibrary::Rm)]
pub struct Rm {
    /// Name of the physical library
    #[proto(PhysicalLibrary)]
    pub name: String,
}

/// Lists the physical libraries (`physicallibrary ls`).
#[typed_admin_cmd(Physicallibrary::Ls, response = PhysicalLibraryLsItem)]
#[derive(Default)]
pub struct Ls {}
