// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The helpers of the conversion tests, and the tests of the common types.
//!
//! The conversion tests of each group (`<group>/tests.rs`) replay the `cta-admin --jsonl` samples of the `cta-sdk` unit
//! tests (`lib/python/cta-sdk/tests/unit/cta/sdk/daos/cta_admin/data/`): each sample is parsed into its protobuf
//! record, converted, and compared with the model the Python DAO tests expect.

use jiff::Timestamp;
use serde::de::DeserializeOwned;

use super::*;
use crate::{
    ConversionError,
    convert::{optional_timestamp, timestamp},
    scheduler::MountType,
    test_support::parse,
};

/// The `Timestamp` of a Unix time in seconds
pub(super) fn ts(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

pub(super) fn log(username: &str, host: &str, time: i64) -> EntryLog {
    EntryLog {
        username: username.into(),
        host: host.into(),
        time: ts(time),
    }
}

pub(super) fn metadata(created: EntryLog, modified: EntryLog, comment: &str) -> Metadata {
    Metadata {
        created,
        modified,
        comment: comment.into(),
    }
}

/// The records of a `cta-admin --jsonl` sample, converted
pub(super) fn convert<R, T>(jsonl: &str) -> Vec<T>
where
    R: DeserializeOwned,
    T: TryFrom<R, Error = ConversionError>,
{
    parse::<R>(jsonl)
        .into_iter()
        .map(|record| T::try_from(record).expect("the record converts"))
        .collect()
}

#[test]
fn mount_type_conversions() {
    use cta_protobuf::cta::admin::MountType as Proto;

    assert_eq!(
        MountType::convert("f", Proto::ArchiveForUser).unwrap(),
        Some(MountType::Archive)
    );
    assert_eq!(
        MountType::convert("f", Proto::ArchiveForRepack).unwrap(),
        Some(MountType::Archive)
    );
    assert_eq!(
        MountType::convert("f", Proto::Retrieve).unwrap(),
        Some(MountType::Retrieve)
    );
    assert_eq!(MountType::convert("f", Proto::NoMount).unwrap(), None);
    assert!(MountType::convert("f", Proto::UnknownMountType).is_err());
    assert_eq!(MountType::Archive.to_string(), "ARCHIVE");
}

#[test]
fn timestamps() {
    assert_eq!(timestamp("f", 0).unwrap(), ts(0));
    assert!(matches!(
        timestamp("f", u64::MAX).unwrap_err(),
        ConversionError::InvalidTimestamp {
            field: "f",
            value: u64::MAX
        }
    ));
    assert_eq!(optional_timestamp("f", 0).unwrap(), None);
    assert_eq!(optional_timestamp("f", 60).unwrap(), Some(ts(60)));
}
