// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of tapes, tape pools and media types, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use cta_protobuf::cta::admin::{MediaTypeLsItem, TapeLsItem, TapePoolLsItem};

use super::*;
use crate::{
    ConversionError,
    test_support::{cta_admin_jsonl, parse},
    tests::{convert, log, metadata, ts},
};

#[test]
fn converts_the_records() {
    let tapes = convert::<TapeLsItem, Tape>(cta_admin_jsonl!("tapes.jsonl"));

    assert_eq!(tapes.len(), 3);
    assert_eq!(
        tapes[0],
        Tape {
            vid: "I71007".into(),
            media_type_name: "3592JE20T".into(),
            vendor: "FUJIFILM".into(),
            purchase_order: String::new(),
            logical_library_name: "IBMLIB3-TS1160".into(),
            physical_library_name: "IBMLIB3".into(),
            tape_pool_name: "supply_alltypes".into(),
            virtual_organisation_name: "IT".into(),
            encryption_key_name: None,
            capacity: 20_000_000_000_000,
            format: LabelFormat::Cta,
            state: TapeState::Active,
            state_reason: String::new(),
            verification_status: Some(VerificationStatus {
                date: ts(1_662_285_645),
                status: "finished".into(),
                file_submitted_count: 2624,
                file_verified_count: 0,
                file_failed_count: 0,
            }),
            statistics: TapeStatistics {
                physical_usage: 0,
                logical_usage: 0,
                is_dirty: false,
                last_fseq: 0,
                is_full: false,
                file_count: 0,
            },
            metadata: TapeMetadata {
                created: log("vlado", "cta-frontend01", 1_649_922_740),
                modified: log("vlado", "ctapreproductionfrontend11", 1_760_953_705),
                comment: String::new(),
                label: Some(TapeLog {
                    drive: "IBMLIB3-TS1160-F14C1R2".into(),
                    time: ts(1_760_953_704),
                }),
                last_written: Some(TapeLog {
                    drive: "IBMLIB3-TS1160-F14C1R2".into(),
                    time: ts(1_739_349_949),
                }),
                last_read: Some(TapeLog {
                    drive: "IBMLIB4-TS1160-F08C2R2".into(),
                    time: ts(1_742_403_814),
                }),
                state_update_time: ts(1_649_922_740),
                state_modified_by: "vlado@cta-frontend01".into(),
                write_mount_count: 99,
                read_mount_count: 0,
            },
        }
    );
    assert!(tapes[0].is_empty());
    assert_eq!(tapes[0].usage_ratio(), 0.0);
    assert_eq!(tapes[0].metadata.metadata().comment, "");
    // no verification status
    assert_eq!(tapes[1].verification_status, None);
    assert_eq!(tapes[2].metadata.comment, "test ATRESYS");
}

#[test]
fn usage_ratio() {
    let mut tape = convert::<TapeLsItem, Tape>(cta_admin_jsonl!("tapes.jsonl")).remove(0);
    tape.statistics.logical_usage = 5_000_000_000_000;
    tape.statistics.physical_usage = 10_000_000_000_000;

    // not full: over the capacity
    assert!((tape.usage_ratio() - 0.25).abs() < 1e-9);
    assert!(!tape.is_empty());

    // full: over what was written
    tape.statistics.is_full = true;
    assert!((tape.usage_ratio() - 0.5).abs() < 1e-9);
}

#[test]
fn rejects_unknown_state() {
    let mut record: TapeLsItem = parse(cta_admin_jsonl!("tapes.jsonl")).remove(0);
    record.state = "LOST".into();

    let error = Tape::try_from(record).unwrap_err();

    assert!(
        matches!(error, ConversionError::InvalidValue { field: "state", ref value } if value == "LOST"),
        "{error}"
    );
}

#[test]
fn rejects_missing_creation_log() {
    let mut record: TapeLsItem = parse(cta_admin_jsonl!("tapes.jsonl")).remove(0);
    record.creation_log = None;

    let error = Tape::try_from(record).unwrap_err();

    assert!(
        matches!(
            error,
            ConversionError::MissingField {
                record: "TapeLsItem",
                field: "creation_log"
            }
        ),
        "{error}"
    );
}

#[test]
fn verification_status_json_round_trip() {
    let status = VerificationStatus {
        date: ts(1_735_689_600),
        status: "finished".into(),
        file_submitted_count: 10,
        file_verified_count: 0,
        file_failed_count: 0,
    };

    let json = status.to_json();

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap(),
        serde_json::json!({
            "date": 1_735_689_600,
            "status": "finished",
            "files_submitted": 10,
            "files_verified": 0,
            "files_failed": 0
        })
    );
    assert_eq!(VerificationStatus::from_json(&json).unwrap(), Some(status));
    assert_eq!(VerificationStatus::from_json("").unwrap(), None);
    assert!(matches!(
        VerificationStatus::from_json("not json").unwrap_err(),
        ConversionError::InvalidJson { .. }
    ));
}

mod pool {
    use super::*;

    #[test]
    fn converts_the_records() {
        let pools = convert::<TapePoolLsItem, TapePool>(cta_admin_jsonl!("tape_pools.jsonl"));

        assert_eq!(pools.len(), 3);
        assert_eq!(
            pools[0],
            TapePool {
                name: "atlaspps".into(),
                virtual_organisation_name: "ATLAS".into(),
                min_writable_tape_count: 1,
                encryption_key_name: None,
                supply_tape_pool_names: vec!["supply_alltypes".into()],
                statistics: TapePoolStatistics {
                    tape_count: 1,
                    file_count: 864,
                    capacity: 18_000_000_000_000,
                    usage: 1_586_358_122_465,
                },
                metadata: metadata(
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_286),
                    log("vlado", "ctapreproductionfrontend11", 1_746_612_191),
                    "tape pool for atlaspps",
                ),
            }
        );
        assert!((pools[0].statistics.usage_percent() - 1_586_358_122_465.0 / 18e12).abs() < 1e-9);
        // an encrypted pool, and an empty one
        assert_eq!(pools[2].encryption_key_name.as_deref(), Some("my_key"));
        assert!(pools[2].supply_tape_pool_names.is_empty());
        assert_eq!(pools[2].statistics.usage_percent(), 0.0);
    }
}

mod media_type {
    use super::*;

    #[test]
    fn converts_the_records() {
        let types = convert::<MediaTypeLsItem, MediaType>(cta_admin_jsonl!("media_types.jsonl"));

        assert_eq!(types.len(), 3);
        assert_eq!(
            types[0],
            MediaType {
                name: "3592JC7T".into(),
                cartridge_type: "3592JC".into(),
                capacity: 7_000_000_000_000,
                primary_density_code: 84,
                secondary_density_code: 0,
                metadata: metadata(
                    log("jleduc", "cta-frontend01.cern.ch", 1_593_464_902),
                    log("jleduc", "cta-frontend01.cern.ch", 1_593_464_902),
                    "IBM 3592JC cartridge formated at 7 TB",
                ),
            }
        );
    }
}
