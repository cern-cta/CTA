// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of disk instances, spaces and systems, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use std::time::Duration;

use cta_protobuf::cta::admin::{DiskInstanceLsItem, DiskInstanceSpaceLsItem, DiskSystemLsItem};

use super::*;
use crate::{
    test_support::cta_admin_jsonl,
    tests::{convert, log, metadata},
    *,
};

#[test]
fn disk_instance() {
    let instances =
        convert::<DiskInstanceLsItem, DiskInstance>(cta_admin_jsonl!("disk_instances.jsonl"));

    assert_eq!(instances.len(), 3);
    assert_eq!(
        instances[0],
        DiskInstance {
            name: "eosctaatlaspps".into(),
            metadata: metadata(
                log("mvelosob", "ctapreproductionfrontend01", 1_651_065_009),
                log("mvelosob", "ctapreproductionfrontend01", 1_651_065_009),
                "eosctaatlaspps instance for existing archive file entries",
            ),
        }
    );
}

#[test]
fn disk_instance_space() {
    let spaces = convert::<DiskInstanceSpaceLsItem, DiskInstanceSpace>(cta_admin_jsonl!(
        "disk_instance_spaces.jsonl"
    ));

    assert_eq!(spaces.len(), 3);
    assert_eq!(
        spaces[0],
        DiskInstanceSpace {
            name: "eosctarepack_default_space".into(),
            disk_instance_name: "eosctarepack".into(),
            free_space_url: "eosSpace:default".into(),
            refresh_interval: Duration::from_secs(10),
            free_space: 145_448_985_042_944,
            metadata: metadata(
                log("jleduc", "cta-frontend01", 1_650_966_505),
                log("jleduc", "cta-frontend01", 1_650_966_505),
                "eosctarepack repack target space",
            ),
        }
    );
    assert_eq!(
        spaces[0].key(),
        ("eosctarepack_default_space", "eosctarepack")
    );
}

#[test]
fn disk_system() {
    let systems = convert::<DiskSystemLsItem, DiskSystem>(cta_admin_jsonl!("disk_systems.jsonl"));

    assert_eq!(systems.len(), 3);
    assert_eq!(
        systems[0],
        DiskSystem {
            name: "ftstest_atlaspps_retrieve".into(),
            disk_instance_name: "eosctaatlaspps".into(),
            disk_instance_space_name: "eosctaatlaspps_default_space".into(),
            file_regex: "/eos/ctaatlaspps/ftstest/.*".into(),
            desired_min_free_space: 1_000_000_000_000,
            sleep_time: Duration::from_secs(3600),
            metadata: metadata(
                log("vyurchen", "cta-frontend01", 1_666_603_959),
                log("vyurchen", "cta-frontend01", 1_666_603_959),
                "free space for ftstest on eosctaatlaspps",
            ),
        }
    );
}
