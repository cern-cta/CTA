// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Validation and encoding of the events.

use std::collections::BTreeMap;

use cta_protobuf::cta::{
    common::checksum_blob::checksum::Type as ProtoChecksumType, eos::workflow::EventType,
};

use super::*;
use crate::file::{Checksum, ChecksumType, Owner};

fn requester() -> Requester {
    Requester::new("user", "group")
}

#[test]
fn archive_needs_a_storage_class() {
    let event = |storage_class: &str| {
        Archive::builder()
            .file(FileInfo::default())
            .storage_class(storage_class)
            .requester(requester())
            .build()
    };
    assert_eq!(event("").validate(), Err(InvalidEvent::MissingStorageClass));
    assert_eq!(event("SC").validate(), Ok(()));
}

#[test]
fn events_on_archived_files_need_an_archive_file_id() {
    for (archive_file_id, expected) in [
        (None, Err(InvalidEvent::MissingArchiveFileId)),
        (Some(0), Err(InvalidEvent::MissingArchiveFileId)),
        (Some(1), Ok(())),
    ] {
        let file = FileInfo {
            archive_file_id,
            ..FileInfo::default()
        };
        let retrieve = Retrieve::builder()
            .file(file.clone())
            .requester(requester())
            .dst_url("root://disk/dst")
            .build();
        let cancel = CancelRetrieve::builder()
            .file(file.clone())
            .requester(requester())
            .request_objectstore_id("id")
            .build();
        let delete = Delete::builder().file(file).requester(requester()).build();
        assert_eq!(retrieve.validate(), expected);
        assert_eq!(cancel.validate(), expected);
        assert_eq!(delete.validate(), expected);
    }
}

/// A file with every field set
fn full_file() -> FileInfo {
    FileInfo::builder()
        .archive_file_id(123)
        .disk_file_id("42")
        .path("/eos/ctaeos/file")
        .owner(Owner(1000, 1001))
        .size(4096)
        .checksums(vec![Checksum::new(
            ChecksumType::Adler32,
            0x0a0b_0c0d_u32.to_le_bytes().to_vec(),
        )])
        .xattr(BTreeMap::from([(
            "user.key".to_owned(),
            "value".to_owned(),
        )]))
        .build()
}

mod notification {
    use super::*;

    #[test]
    fn common_fields() {
        let event = Delete::builder()
            .file(full_file())
            .requester(requester())
            .build();

        let notification = event.notification("eosdev").unwrap();

        let wf = notification.wf.unwrap();
        assert_eq!(wf.event(), EventType::Delete);
        assert_eq!(wf.requester_instance, REQUESTER_INSTANCE);
        let instance = wf.instance.unwrap();
        assert_eq!(
            (instance.name.as_str(), instance.url.as_str()),
            ("eosdev", "")
        );
        let user = notification.cli.unwrap().user.unwrap();
        assert_eq!(
            (user.username.as_str(), user.groupname.as_str()),
            ("user", "group")
        );
        let file = notification.file.unwrap();
        assert_eq!(file.archive_file_id, 123);
        assert_eq!(file.disk_file_id, "42");
        assert_eq!(file.lpath, "/eos/ctaeos/file");
        assert_eq!(file.size, 4096);
        let owner = file.owner.unwrap();
        assert_eq!((owner.uid, owner.gid), (1000, 1001));
        let checksums = file.csb.unwrap().cs;
        assert_eq!(checksums.len(), 1);
        assert_eq!(checksums[0].r#type(), ProtoChecksumType::Adler32);
        assert_eq!(checksums[0].value, vec![0x0d, 0x0c, 0x0b, 0x0a]);
        assert_eq!(
            file.xattr.get("user.key").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn unset_fields_are_defaults() {
        let event = Create::builder()
            .file(FileInfo::default())
            .storage_class("SC")
            .requester(requester())
            .build();

        let file = event.notification("eosdev").unwrap().file.unwrap();

        assert_eq!(file.archive_file_id, 0);
        assert!(file.csb.is_none());
        assert!(file.owner.is_none());
        assert_eq!(file.storage_class, "SC");
    }

    #[test]
    fn archive() {
        let event = Archive::builder()
            .file(full_file())
            .storage_class("SC")
            .requester(requester())
            .service_url("root://eos")
            .report_url("eosQuery://report")
            .error_report_url("eosQuery://error")
            .build();

        let notification = event.notification("eosdev").unwrap();

        let wf = notification.wf.unwrap();
        assert_eq!(wf.event(), EventType::Closew);
        assert_eq!(wf.instance.unwrap().url, "root://eos");
        let transport = notification.transport.unwrap();
        assert_eq!(transport.report_url, "eosQuery://report");
        assert_eq!(transport.error_report_url, "eosQuery://error");
        assert_eq!(notification.file.unwrap().storage_class, "SC");
    }

    #[test]
    fn retrieve() {
        let event = Retrieve::builder()
            .file(full_file())
            .requester(requester())
            .dst_url("root://disk/dst")
            .vid("V01001")
            .verify_only(true)
            .error_report_url("eosQuery://error")
            .build();

        let notification = event.notification("eosdev").unwrap();

        let wf = notification.wf.unwrap();
        assert_eq!(wf.event(), EventType::Prepare);
        assert_eq!(wf.vid, "V01001");
        assert!(wf.verify_only);
        let transport = notification.transport.unwrap();
        assert_eq!(transport.dst_url, "root://disk/dst");
        assert_eq!(transport.error_report_url, "eosQuery://error");
    }

    #[test]
    fn cancel_retrieve() {
        let event = CancelRetrieve::builder()
            .file(full_file())
            .requester(requester())
            .request_objectstore_id("store-abc")
            .build();

        let notification = event.notification("eosdev").unwrap();

        assert_eq!(notification.wf.unwrap().event(), EventType::AbortPrepare);
        assert_eq!(
            notification.file.unwrap().request_objectstore_id,
            "store-abc"
        );
    }

    #[test]
    fn invalid_events_are_not_encoded() {
        let event = Delete::builder()
            .file(FileInfo::default())
            .requester(requester())
            .build();

        assert_eq!(
            event.notification("eosdev"),
            Err(InvalidEvent::MissingArchiveFileId)
        );
    }
}
