// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Media types

use cta_protobuf::cta::admin::MediaTypeLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A kind of cartridge and the format it is written in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct MediaType {
    /// Name of the media type
    pub name: String,
    /// Cartridge type
    pub cartridge_type: String,
    /// Capacity, in bytes
    pub capacity: u64,
    /// Primary density code
    pub primary_density_code: u64,
    /// Secondary density code
    pub secondary_density_code: u64,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<MediaTypeLsItem> for MediaType {
    type Error = ConversionError;

    fn try_from(item: MediaTypeLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            cartridge_type: item.cartridge,
            capacity: item.capacity,
            primary_density_code: item.primary_density_code,
            secondary_density_code: item.secondary_density_code,
            metadata: Metadata::convert(
                "MediaTypeLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
