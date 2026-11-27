// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Types shared by several resources

use std::{fmt::Debug, hash::Hash};

use cta_protobuf::cta::common;
use jiff::Timestamp;
use serde::Serialize;

use crate::{ConversionError, convert::timestamp};

/// A resource of the catalogue or of the scheduler, identified by a (possibly composite) key.
///
/// Two resources with the same key are the same resource, even when their other fields differ (e.g. the same tape
/// pool listed before and after a change).
///
/// ```
/// use cta_model::{Keyed, tape::TapePool};
///
/// fn same_pool(a: &TapePool, b: &TapePool) -> bool {
///     a.key() == b.key()
/// }
/// ```
pub trait Keyed {
    /// The key identifying the resource
    type Key<'a>: Hash + Eq + Debug
    where
        Self: 'a;

    /// The key of this resource.
    fn key(&self) -> Self::Key<'_>;
}

/// Who did something, from where and when (the `EntryLog` of the catalogue).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntryLog {
    /// User name
    pub user_name: String,
    /// Host name
    pub host_name: String,
    /// When
    pub time: Timestamp,
}

impl EntryLog {
    /// Converts the protobuf log, `field` naming it in the error.
    pub(crate) fn convert(
        field: &'static str,
        log: common::EntryLog,
    ) -> Result<Self, ConversionError> {
        Ok(Self {
            user_name: log.username,
            host_name: log.host,
            time: timestamp(field, log.time)?,
        })
    }
}

/// The creation and modification logs of a resource, and its comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Metadata {
    /// Who created the resource, from where and when
    pub creation: EntryLog,
    /// Who last modified the resource, from where and when
    pub update: EntryLog,
    /// Free-text comment of the operators
    pub comment: String,
}

impl Metadata {
    /// Builds the metadata of a `record` from its logs, which must both be present.
    pub(crate) fn convert(
        record: &'static str,
        creation: Option<common::EntryLog>,
        update: Option<common::EntryLog>,
        comment: String,
    ) -> Result<Self, ConversionError> {
        Ok(Self {
            creation: EntryLog::convert(
                "creation_log",
                creation.ok_or(ConversionError::MissingField {
                    record,
                    field: "creation_log",
                })?,
            )?,
            update: EntryLog::convert(
                "last_modification_log",
                update.ok_or(ConversionError::MissingField {
                    record,
                    field: "last_modification_log",
                })?,
            )?,
            comment,
        })
    }
}
