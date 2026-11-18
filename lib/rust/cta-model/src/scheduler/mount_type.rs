// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The direction of a mount, shared by the drives and the queues

use cta_protobuf::cta::admin::{MountType as ProtoMountType, RequestType};
use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

use crate::ConversionError;

/// The direction of a mount: writing to tape or reading from it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MountType {
    /// Data is written to tape
    Archive,
    /// Data is read from tape
    Retrieve,
    /// The tape is labelled
    Label,
}

impl MountType {
    /// The mount type of a drive or of a queue, `None` when nothing is mounted.
    pub(crate) fn convert(
        field: &'static str,
        value: ProtoMountType,
    ) -> Result<Option<Self>, ConversionError> {
        Ok(Some(match value {
            ProtoMountType::NoMount => return Ok(None),
            ProtoMountType::ArchiveForUser
            | ProtoMountType::ArchiveForRepack
            | ProtoMountType::ArchiveAllTypes => Self::Archive,
            ProtoMountType::Retrieve => Self::Retrieve,
            ProtoMountType::Label => Self::Label,
            ProtoMountType::UnknownMountType => {
                return Err(ConversionError::invalid_value(field, value.as_str_name()));
            }
        }))
    }

    /// The mount type of a request; the `TOTAL` pseudo-type of the summaries has none.
    pub(crate) fn convert_request_type(
        field: &'static str,
        value: RequestType,
    ) -> Result<Self, ConversionError> {
        match value {
            RequestType::ArchiveRequest => Ok(Self::Archive),
            RequestType::RetrieveRequest => Ok(Self::Retrieve),
            RequestType::Total => Err(ConversionError::invalid_value(field, value.as_str_name())),
        }
    }
}
