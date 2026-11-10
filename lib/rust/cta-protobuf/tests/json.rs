// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The proto3 JSON produced by the generated serde impls must match what
//! `cta-admin --jsonl` prints for the same message, byte for byte: protobuf's
//! JSON printer with `always_print_fields_with_no_presence`, i.e.
//! lowerCamelCase names in field order, 64-bit integers as strings, enums as
//! names, fields without presence printed even when they hold their default
//! value, and unset message fields left out.
//!
//! The samples are the `cta-admin --jsonl` output used by the `cta-sdk` unit
//! tests.

use cta_protobuf::cta::admin::{DriveLsItem, TapeLsItem, TapePoolLsItem, Version, VersionItem};
use serde::{Serialize, de::DeserializeOwned};

/// Sample `cta-admin --jsonl` output from the `cta-sdk` unit tests.
macro_rules! cta_admin_jsonl {
    ($file:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../python/cta-sdk/tests/unit/cta/sdk/daos/cta_admin/data/",
            $file
        ))
    };
}

/// Parses every line of `jsonl` as `T` and checks that serializing it gives
/// back the same line.
fn assert_round_trip<T: Serialize + DeserializeOwned>(jsonl: &str) {
    let mut count = 0;
    for line in jsonl.lines().filter(|line| !line.trim().is_empty()) {
        let item: T = serde_json::from_str(line).expect("valid cta-admin JSON");
        assert_eq!(serde_json::to_string(&item).unwrap(), line);
        count += 1;
    }
    assert!(count > 0, "no JSON lines in the sample");
}

#[test]
fn tape_ls_item() {
    assert_round_trip::<TapeLsItem>(cta_admin_jsonl!("tapes.jsonl"));
}

#[test]
fn tape_pool_ls_item() {
    assert_round_trip::<TapePoolLsItem>(cta_admin_jsonl!("tape_pools.jsonl"));
}

#[test]
fn drive_ls_item() {
    assert_round_trip::<DriveLsItem>(cta_admin_jsonl!("drives.jsonl"));
}

#[test]
fn version_item() {
    let item = VersionItem {
        client_version: Some(Version {
            cta_version: "5.11.12.1-1".to_owned(),
            xrootd_ssi_protobuf_interface_version: "v1.17".to_owned(),
        }),
        server_version: None,
        catalogue_connection_string: "postgresql:postgresql://cta@db/cta".to_owned(),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_string(&item).unwrap(),
        concat!(
            r#"{"clientVersion":{"ctaVersion":"5.11.12.1-1","xrootdSsiProtobufInterfaceVersion":"v1.17"},"#,
            r#""catalogueConnectionString":"postgresql:postgresql://cta@db/cta","catalogueVersion":"","#,
            r#""isUpgrading":false,"schedulerBackendName":"","instanceName":""}"#,
        )
    );
}
