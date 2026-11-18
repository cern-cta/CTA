// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Tape pools

use cta_protobuf::cta::admin::TapePoolLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata, convert::non_empty};

/// Usage statistics of a tape pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TapePoolStatistics {
    /// Number of tapes in the pool
    pub tape_count: u64,
    /// Number of files in the pool
    pub file_count: u64,
    /// Capacity of the pool, in bytes
    pub capacity: u64,
    /// Bytes stored in the pool
    pub usage: u64,
}

impl TapePoolStatistics {
    /// The used fraction of the capacity (between 0 and 1; 0 for an empty pool).
    #[must_use]
    pub fn usage_percent(&self) -> f64 {
        self.usage as f64 / self.capacity.max(1) as f64
    }
}

/// A group of tapes written together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct TapePool {
    /// Name of the tape pool
    pub name: String,
    /// Virtual organisation owning the pool
    pub virtual_organisation_name: String,
    /// Number of partially filled tapes the pool should keep for writing
    pub min_writable_tape_count: u64,
    /// Name of the key the tapes of the pool are encrypted with (`None`: no encryption)
    pub encryption_key_name: Option<String>,
    /// Pools supplying tapes to this one when it runs out of writable tapes
    pub supply_tape_pool_names: Vec<String>,
    /// Usage statistics
    pub statistics: TapePoolStatistics,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<TapePoolLsItem> for TapePool {
    type Error = ConversionError;

    fn try_from(item: TapePoolLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            virtual_organisation_name: item.vo,
            min_writable_tape_count: item.num_partial_tapes,
            encryption_key_name: non_empty(item.encryption_key_name),
            supply_tape_pool_names: item.supply_source,
            statistics: TapePoolStatistics {
                tape_count: item.num_tapes,
                file_count: item.num_physical_files,
                capacity: item.capacity_bytes,
                usage: item.data_bytes,
            },
            metadata: Metadata::convert(
                "TapePoolLsItem",
                item.created,
                item.modified,
                item.comment,
            )?,
        })
    }
}
