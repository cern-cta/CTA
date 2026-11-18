// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Physical libraries

use cta_protobuf::cta::admin::PhysicalLibraryLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata, convert::non_empty, library::LibraryState};

/// A tape library.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct PhysicalLibrary {
    /// Name of the library
    pub name: String,
    /// Manufacturer
    pub manufacturer: String,
    /// Model
    pub model: String,
    /// Cartridge technology
    pub library_type: String,
    /// URL of the management interface
    pub gui_url: String,
    /// URL of the webcam
    pub webcam_url: String,
    /// Where the library is
    pub location: String,
    /// Number of cartridge slots
    pub physical_cartridge_slots: u64,
    /// Number of free cartridge slots
    pub available_cartridge_slots: u64,
    /// Number of drive slots
    pub physical_drive_slots: u64,
    /// Whether its tapes can be mounted
    pub state: LibraryState,
    /// Reason given for disabling it, if any
    pub state_reason: Option<String>,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<PhysicalLibraryLsItem> for PhysicalLibrary {
    type Error = ConversionError;

    fn try_from(item: PhysicalLibraryLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            manufacturer: item.manufacturer,
            model: item.model,
            library_type: item.r#type,
            gui_url: item.gui_url,
            webcam_url: item.webcam_url,
            location: item.location,
            physical_cartridge_slots: item.nb_physical_cartridge_slots,
            available_cartridge_slots: item.nb_available_cartridge_slots,
            physical_drive_slots: item.nb_physical_drive_slots,
            state: LibraryState::from_disabled(item.is_disabled),
            state_reason: non_empty(item.disabled_reason),
            metadata: Metadata::convert(
                "PhysicalLibraryLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
