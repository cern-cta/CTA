// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Physical library commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a physical library (`physicallibrary add`).
    #[proto(Physicallibrary.SubcmdAdd)]
    pub struct Add {
        /// Name of the physical library
        name: str => PhysicalLibrary,
        /// Manufacturer of the library
        manufacturer: str => Manufacturer,
        /// Model of the library
        model: str => LibraryModel,
        /// Type of the library
        library_type: str? => LibraryType,
        /// URL of the library GUI
        gui_url: str? => GuiUrl,
        /// URL of the library webcam
        webcam_url: str? => WebcamUrl,
        /// Location of the library
        location: str? => LibraryLocation,
        /// Number of physical cartridge slots
        nb_physical_cartridge_slots: u64 => NbPhysicalCartridgeSlots,
        /// Number of available cartridge slots
        nb_available_cartridge_slots: u64? => NbAvailableCartridgeSlots,
        /// Number of physical drive slots
        nb_physical_drive_slots: u64 => NbPhysicalDriveSlots,
        /// Comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a physical library (`physicallibrary ch`).
    #[proto(Physicallibrary.SubcmdCh)]
    pub struct Ch {
        /// Name of the physical library
        name: str => PhysicalLibrary,
        /// New model
        model: str? => LibraryModel,
        /// New type
        library_type: str? => LibraryType,
        /// New URL of the library GUI
        gui_url: str? => GuiUrl,
        /// New URL of the library webcam
        webcam_url: str? => WebcamUrl,
        /// New location
        location: str? => LibraryLocation,
        /// New number of physical cartridge slots
        nb_physical_cartridge_slots: u64? => NbPhysicalCartridgeSlots,
        /// New number of available cartridge slots
        nb_available_cartridge_slots: u64? => NbAvailableCartridgeSlots,
        /// New number of physical drive slots
        nb_physical_drive_slots: u64? => NbPhysicalDriveSlots,
        /// New comment
        comment: str? => Comment,
        /// Whether the library is disabled
        disabled: bool? => Disabled,
        /// Why the library is disabled
        disabled_reason: str? => DisabledReason,
    }
}

define_typed_admin_cmd! {
    /// Removes a physical library (`physicallibrary rm`).
    #[proto(Physicallibrary.SubcmdRm)]
    pub struct Rm {
        /// Name of the physical library
        name: str => PhysicalLibrary,
    }
}

define_typed_admin_cmd! {
    /// Lists the physical libraries (`physicallibrary ls`).
    #[proto(Physicallibrary.SubcmdLs)]
    #[item(PllsItem: PhysicalLibraryLsItem)]
    #[derive(Default)]
    pub struct Ls {}
}
