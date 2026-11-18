// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Logical libraries

use cta_protobuf::cta::admin::LogicalLibraryLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata, convert::non_empty, library::LibraryState};

/// A logical library: the tapes and drives of a physical library that can be mounted together
/// (`logicallibrary ls`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct LogicalLibrary {
    /// Name of the logical library
    pub name: String,
    /// Physical library hosting it
    pub physical_library_name: String,
    /// Whether its tapes can be mounted
    pub state: LibraryState,
    /// Reason given for disabling it, if any
    pub state_reason: Option<String>,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<LogicalLibraryLsItem> for LogicalLibrary {
    type Error = ConversionError;

    fn try_from(item: LogicalLibraryLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            physical_library_name: item.physical_library,
            state: LibraryState::from_disabled(item.is_disabled),
            state_reason: non_empty(item.disabled_reason),
            metadata: Metadata::convert(
                "LogicalLibraryLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
