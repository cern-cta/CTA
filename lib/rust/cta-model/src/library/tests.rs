// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of drives and libraries, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use std::time::Duration;

use cta_protobuf::cta::admin::{DriveLsItem, LogicalLibraryLsItem, PhysicalLibraryLsItem};

use super::*;
use crate::{
    scheduler::MountType,
    test_support::{cta_admin_jsonl, parse},
    tests::{convert, log, metadata},
};

mod drive {
    use super::*;

    #[test]
    fn converts_the_records() {
        let drives = convert::<DriveLsItem, Drive>(cta_admin_jsonl!("drives.jsonl"));

        assert_eq!(drives.len(), 3);
        let drive = &drives[0];
        let expected_config: std::collections::BTreeMap<String, String> = [
            ("taped.DaemonUserName", "cta"),
            ("taped.LogMask", "INFO"),
            (
                "ObjectStore.BackendPath",
                "rados://cta-production@tapecta:cta-production",
            ),
            ("environment.XrdSecPROTOCOL", "sss"),
            ("general.SchedulerBackendName", "cephUser"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        assert_eq!(drive.config.len(), 34);
        for (key, value) in &expected_config {
            assert_eq!(drive.config.get(key), Some(value), "{key}");
        }
        assert_eq!(
            Drive {
                config: expected_config,
                ..drive.clone()
            },
            Drive {
                name: "SPECTRALIB3-LTO9-F10B1S1".into(),
                logical_library_name: "SPECTRALIB3-LTO9".into(),
                host_name: "tpsrv640".into(),
                desired_state: DriveOperatingState::Up,
                state: DriveOperatingState::Up,
                state_reason: String::new(),
                device_path: "/dev/tape/by-name/SPECTRALIB3-LTO9-F10B1S1".into(),
                physical_library_name: "SPECTRALIB3".into(),
                physical_library_slot: "smc3".into(),
                tape_session: Some(TapeSession {
                    id: 2_089_339,
                    vid: "L96884".into(),
                    tape_pool_name: "vo_NA62_2".into(),
                    mount_type: MountType::Retrieve,
                    file_transferred_count: 54,
                    byte_transferred_count: 318_701_085_660,
                    virtual_organisation_name: "NA62".into(),
                    state: TapeSessionState::Transferring,
                    duration: Duration::from_secs(832),
                }),
                state_change_age: Duration::from_secs(832),
                config: Drive { ..drive.clone() }
                    .config
                    .into_iter()
                    .filter(|(k, _)| {
                        [
                            "taped.DaemonUserName",
                            "taped.LogMask",
                            "ObjectStore.BackendPath",
                            "environment.XrdSecPROTOCOL",
                            "general.SchedulerBackendName",
                        ]
                        .contains(&k.as_str())
                    })
                    .collect(),
                metadata: DriveMetadata {
                    last_refresh_age: Duration::from_secs(14),
                    cta_version: "5.11.12.1-1".into(),
                    scheduler_namespace: "cephUser".into(),
                    comment: "New Drive".into(),
                },
            }
        );
        let session = drive.tape_session.as_ref().unwrap();
        assert!((session.speed() - 318_701_085_660.0 / 832.0).abs() < 1e-6);
        // an archive session
        assert_eq!(
            drives[1].tape_session.as_ref().unwrap().mount_type,
            MountType::Archive
        );
    }

    #[test]
    fn without_session() {
        let mut record: DriveLsItem = parse(cta_admin_jsonl!("drives.jsonl")).remove(0);
        record.set_mount_type(cta_protobuf::cta::admin::MountType::NoMount);
        record.set_drive_status(cta_protobuf::cta::admin::drive_ls_item::DriveStatus::Down);
        record.set_desired_drive_state(cta_protobuf::cta::admin::drive_ls_item::DriveStatus::Down);
        record.reason = "maintenance".into();

        let drive = Drive::try_from(record).unwrap();

        assert_eq!(drive.tape_session, None);
        assert_eq!(drive.state, DriveOperatingState::Down);
        assert_eq!(drive.desired_state, DriveOperatingState::Down);
        assert_eq!(drive.state_reason, "maintenance");
    }
}

mod logical {
    use super::*;

    #[test]
    fn converts_the_records() {
        let libraries = convert::<LogicalLibraryLsItem, LogicalLibrary>(cta_admin_jsonl!(
            "logical_libraries.jsonl"
        ));

        assert_eq!(libraries.len(), 3);
        assert_eq!(
            libraries[0],
            LogicalLibrary {
                name: "IBM1L9".into(),
                physical_library_name: "IBMLIB1".into(),
                state: LibraryState::Enabled,
                state_reason: None,
                metadata: metadata(
                    log("CASTOR", "CASTOR", 1_630_057_931),
                    log("idlarbi", "ctapreproductionfrontend11", 1_752_224_263),
                    "IBMLIB1 (IBM TS4500 tape library in B513) - LTO-9 tape drives",
                ),
            }
        );
        assert_eq!(libraries[2].state, LibraryState::Disabled);
        assert_eq!(
            libraries[2].state_reason.as_deref(),
            Some("test disable feature")
        );
    }
}

mod physical {
    use super::*;

    #[test]
    fn converts_the_records() {
        let libraries = convert::<PhysicalLibraryLsItem, PhysicalLibrary>(cta_admin_jsonl!(
            "physical_libraries.jsonl"
        ));

        assert_eq!(libraries.len(), 3);
        assert_eq!(
            libraries[0],
            PhysicalLibrary {
                name: "IBMLIB1".into(),
                manufacturer: "IBM".into(),
                model: "TS4500".into(),
                r#type: "LTO".into(),
                gui_url: "http://ibmlib1.cern.ch/".into(),
                webcam_url: String::new(),
                location: "513-S-034".into(),
                physical_cartridge_slots: 22_356,
                available_cartridge_slots: 22_356,
                physical_drive_slots: 48,
                state: LibraryState::Enabled,
                state_reason: None,
                metadata: metadata(
                    log("vlado", "cta-frontend01", 1_689_326_540),
                    log("vlado", "cta-frontend01", 1_706_790_491),
                    "",
                ),
            }
        );
        assert_eq!(libraries[2].state, LibraryState::Disabled);
        assert_eq!(libraries[2].state_reason.as_deref(), Some("test feature"));
    }
}
