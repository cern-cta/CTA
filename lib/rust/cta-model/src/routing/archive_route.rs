// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archive routes

use cta_protobuf::cta::admin::{
    ArchiveRouteLsItem, archive_route_ls_item::ArchiveRouteType as ProtoArchiveRouteType,
};
use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

use crate::{ConversionError, Keyed, Metadata};

/// The workflow an archive route serves.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ArchiveRouteType {
    /// Files archived by users
    Default,
    /// Files archived by repacks
    Repack,
}

impl From<ProtoArchiveRouteType> for ArchiveRouteType {
    fn from(value: ProtoArchiveRouteType) -> Self {
        match value {
            ProtoArchiveRouteType::Default => Self::Default,
            ProtoArchiveRouteType::Repack => Self::Repack,
        }
    }
}

/// Where a copy of the files of a storage class is written.
///
/// Identified by its storage class, copy number and type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Keyed)]
#[key(storage_class_name, copy_number, r#type)]
pub struct ArchiveRoute {
    /// Storage class of the files
    pub storage_class_name: String,
    /// Copy number, between 1 and the number of copies of the storage class
    pub copy_number: u32,
    /// Workflow the route serves
    pub r#type: ArchiveRouteType,
    /// Tape pool the copy is written to
    pub tape_pool_name: String,
    /// Creation and modification logs, comment
    pub metadata: Metadata,
}

impl TryFrom<ArchiveRouteLsItem> for ArchiveRoute {
    type Error = ConversionError;

    fn try_from(item: ArchiveRouteLsItem) -> Result<Self, Self::Error> {
        let route_type = item.archive_route_type().into();
        Ok(Self {
            storage_class_name: item.storage_class,
            copy_number: item.copy_number,
            r#type: route_type,
            tape_pool_name: item.tapepool,
            metadata: Metadata::convert(
                "ArchiveRouteLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
