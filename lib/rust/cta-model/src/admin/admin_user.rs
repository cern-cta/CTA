// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Administrators

use cta_protobuf::cta::admin::AdminLsItem;
use serde::Serialize;

use crate::{ConversionError, Keyed, Metadata};

/// A user allowed to run admin commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(name)]
pub struct AdminUser {
    /// User name
    pub name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<AdminLsItem> for AdminUser {
    type Error = ConversionError;

    fn try_from(item: AdminLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.user,
            metadata: Metadata::convert(
                "AdminLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
