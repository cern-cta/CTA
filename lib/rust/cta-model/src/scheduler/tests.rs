// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of the scheduler requests, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use std::time::Duration;

use cta_protobuf::cta::admin::{
    FailedRequestLsItem, FailedRequestLsSummary, RepackLsItem, ShowQueuesItem,
};

use super::*;
use crate::{
    ConversionError,
    file::{ArchiveFile, DiskFile, TapeFile},
    test_support::{cta_admin_jsonl, parse},
    tests::{convert, log, ts},
    *,
};

mod failed_request {
    use super::*;

    #[test]
    fn converts_the_records() {
        let requests = convert::<FailedRequestLsItem, FailedRequest>(cta_admin_jsonl!(
            "failed_requests.jsonl"
        ));

        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0],
            FailedRequest {
                id: "RetrieveRequest-Frontend-ctapreproductionfrontend11.cern.ch-3560801-20250611-12:55:20-0-3"
                    .into(),
                mount_type: MountType::Retrieve,
                requester_name: "ctaops".into(),
                requester_group: "adm".into(),
                archive_file: ArchiveFile {
                    archive_id: 4_299_888_572,
                    storage_class_name: String::new(),
                    creation_time: ts(1_749_653_171),
                    size: 1_024_000,
                    checksums: vec![],
                },
                disk_file: DiskFile {
                    disk_instance_name: String::new(),
                    id: String::new(),
                    path: "/eos/ctaatlaspps/ftstest/IBM460/a75bd88e-8c11-4652-a672-b6a268968343/0/test00000005"
                        .into(),
                    id_when_deleted: None,
                    owner: None,
                },
                tape_file: Some(TapeFile {
                    vid: "I72509".into(),
                    copy_number: 1,
                    block_id: 0,
                    fseq: 1,
                }),
                tape_pool_name: String::new(),
                total_retries: 0,
                logs: vec![],
            }
        );
    }

    #[test]
    fn without_tape_file() {
        let mut record: FailedRequestLsItem =
            parse(cta_admin_jsonl!("failed_requests.jsonl")).remove(0);
        record.tf = None;
        record.set_request_type(cta_protobuf::cta::admin::RequestType::ArchiveRequest);

        let request = FailedRequest::try_from(record).unwrap();

        assert_eq!(request.tape_file, None);
        assert_eq!(request.mount_type, MountType::Archive);
    }

    #[test]
    fn summary() {
        let summary = FailedRequestLsSummary {
            request_type: cta_protobuf::cta::admin::RequestType::ArchiveRequest.into(),
            total_files: 12,
            total_size: 34,
            scheduler_backend_name: "cephUser".into(),
            ..Default::default()
        };

        assert_eq!(
            FailedRequestSummary::try_from(summary.clone()).unwrap(),
            FailedRequestSummary {
                mount_type: MountType::Archive,
                total_file_count: 12,
                total_byte_count: 34,
                scheduler_backend_name: "cephUser".into(),
            }
        );

        // the overall total has no direction
        let total = FailedRequestLsSummary {
            request_type: cta_protobuf::cta::admin::RequestType::Total.into(),
            ..summary
        };
        assert!(matches!(
            FailedRequestSummary::try_from(total).unwrap_err(),
            ConversionError::InvalidValue {
                field: "request_type",
                ..
            }
        ));
    }
}

mod repack_request {
    use super::*;

    #[test]
    fn converts_the_records() {
        let repacks = convert::<RepackLsItem, RepackRequest>(cta_admin_jsonl!("repacks.jsonl"));

        assert_eq!(repacks.len(), 2);
        assert_eq!(
            repacks[0],
            RepackRequest {
                vid: "I71576".into(),
                tape_pool_name: "test_IT_rucio".into(),
                state: RepackState::Failed,
                failed_file_count: 7,
                disk_buffer_url: "root://eosctarepack//eos/ctarepack/tmp".into(),
                duration: Duration::from_secs(2959),
                finished_time: Some(ts(1_760_794_527)),
                destination_infos: vec![],
                last_expanded_fseq: 57,
                start_info: RepackStartInfo {
                    file_count: 7,
                    byte_count: 15_180_950_480,
                    creation: log("vlado", "ctapreproductionfrontend11", 1_760_791_568),
                    is_all_files_selected_at_start: true,
                },
                retrieve_statistics: RepackStatistics {
                    total_file_count: 7,
                    total_byte_count: 15_180_950_480,
                    transferred_file_count: 0,
                    transferred_byte_count: 0,
                    failed_file_count: 7,
                    failed_byte_count: 15_180_950_480,
                },
                archive_statistics: RepackStatistics {
                    total_file_count: 7,
                    total_byte_count: 15_180_950_480,
                    transferred_file_count: 0,
                    transferred_byte_count: 0,
                    failed_file_count: 0,
                    failed_byte_count: 0,
                },
            }
        );
        assert_eq!(repacks[0].progress(), 0.0);
        assert_eq!(repacks[0].retrieve_statistics.remaining_file_count(), 7);
        // a running repack, half retrieved
        assert_eq!(repacks[1].state, RepackState::Running);
        assert_eq!(repacks[1].finished_time, None);
        assert!((repacks[1].progress() - 0.5).abs() < 1e-9);
        assert_eq!(repacks[1].archive_statistics.remaining_byte_count(), 60);
    }

    #[test]
    fn state_names() {
        assert_eq!(
            "ToExpand".parse::<RepackState>().unwrap(),
            RepackState::ToExpand
        );
        assert_eq!(
            "Pending".parse::<RepackState>().unwrap(),
            RepackState::Pending
        );
        assert!("Unknown".parse::<RepackState>().is_err());
    }
}

mod user_request {
    use super::*;

    #[test]
    fn converts_the_records() {
        let requests = convert::<ShowQueuesItem, UserRequest>(cta_admin_jsonl!("requests.jsonl"));

        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0],
            UserRequest {
                mount_type: MountType::Archive,
                tape_pool_name: "backup_HDFS".into(),
                vid: None,
                virtual_organisation_name: "BACKUP_HDFS".into(),
                logical_library_name: String::new(),
                queued_file_count: 9,
                queued_byte_count: 13_719_009_715,
                oldest_age: Duration::from_secs(1563),
                priority: 250,
                min_age: Duration::from_secs(14_400),
                mount_count: 0,
                transferred_file_count: 0,
                transferred_byte_count: 0,
                is_sleeping_for_space: false,
                sleep_start_time: None,
                scheduler_backend_name: "cephUser".into(),
                tapes_file_count: 3618,
                tapes_byte_count: 121_932_120_767_281,
                tapes_capacity: 121_000_000_000_000,
            }
        );
        assert_eq!(requests[0].key(), (MountType::Archive, "backup_HDFS", None));
    }

    #[test]
    fn summary() {
        let requests = convert::<ShowQueuesItem, UserRequest>(cta_admin_jsonl!("requests.jsonl"));

        let summaries =
            summarize(&requests, GroupBy::TapePoolName, OrderBy::QueuedByteCount).unwrap();

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].group_value, "vo_AMS");
        assert_eq!(
            summaries[1],
            UserRequestSummary {
                group_by: GroupBy::TapePoolName,
                group_value: "backup_HDFS".into(),
                min_age: Duration::from_secs(14_400),
                oldest_age: Duration::from_secs(7945),
                queued_file_count: 11,
                queued_byte_count: 13_719_010_203,
                mount_count: 0,
                transferred_file_count: 0,
                transferred_byte_count: 0,
                tapes_count: None,
                tapes_file_count: 8_322_890,
                tapes_byte_count: 18_679_761_883_902_038,
                tapes_capacity: 12_397_000_000_000_000,
                highest_priority: 250,
            }
        );
        assert_eq!(summaries[1].key(), (GroupBy::TapePoolName, "backup_HDFS"));
    }

    #[test]
    fn summary_counts_the_tapes_of_retrieves() {
        let mut requests =
            convert::<ShowQueuesItem, UserRequest>(cta_admin_jsonl!("requests.jsonl"));
        for (request, vid) in requests.iter_mut().zip(["V1", "V2", "V2"]) {
            request.mount_type = MountType::Retrieve;
            request.vid = Some(vid.into());
            request.logical_library_name = "lib".into();
        }

        let summaries =
            summarize(&requests, GroupBy::LogicalLibraryName, OrderBy::TapesCount).unwrap();

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].tapes_count, Some(2));
        assert_eq!(summaries[0].queued_file_count, 25);
    }

    #[test]
    fn summary_rejects_mixed_directions() {
        let mut requests =
            convert::<ShowQueuesItem, UserRequest>(cta_admin_jsonl!("requests.jsonl"));
        requests[1].mount_type = MountType::Retrieve;

        assert_eq!(
            summarize(&requests, GroupBy::TapePoolName, OrderBy::MinAge),
            Err(MixedMountTypesError)
        );
        assert_eq!(
            summarize(&[], GroupBy::TapePoolName, OrderBy::MinAge),
            Ok(vec![])
        );
    }
}
