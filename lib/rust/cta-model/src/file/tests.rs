// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of files and checksums, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use cta_protobuf::cta::admin::{RecycleTapeFileLsItem, TapeFileLsItem, tape_file_ls_item};

use super::*;
use crate::{
    ConversionError,
    test_support::cta_admin_jsonl,
    tests::{convert, ts},
    *,
};

#[test]
fn converts_the_records() {
    let files = convert::<TapeFileLsItem, File>(cta_admin_jsonl!("tape_files.jsonl"));

    assert_eq!(files.len(), 3);
    assert_eq!(
        files[0],
        File {
            archive_file: ArchiveFile {
                id: 4_303_580_667,
                storage_class: "digital_memory".into(),
                creation_time: ts(1_677_166_670),
                size: 5,
                checksums: vec![Checksum::new(
                    ChecksumType::Adler32,
                    vec![0x05, 0x88, 0x01, 0xab]
                )],
            },
            tape_file: TapeFile {
                vid: "I72052".into(),
                copy_nb: 1,
                block_id: 0,
                f_seq: 1,
            },
            disk_file: DiskFile {
                instance: "eosctapublicpps".into(),
                id: "4296353178".into(),
                path: String::new(),
                id_when_deleted: None,
                owner: Some(Owner(104_039, 2766)),
            },
        }
    );
    assert_eq!(files[0].archive_file.checksums[0].hex_value(), "058801ab");
    assert_eq!(files[0].key(), (4_303_580_667, "I72052", 1));
    // the second copy of the same file is another key
    assert_ne!(files[0].key(), files[1].key());
}

#[test]
fn rejects_incomplete_record() {
    let item = TapeFileLsItem {
        af: Some(Default::default()),
        df: Some(Default::default()),
        tf: None,
        ..Default::default()
    };

    let error = File::try_from(item).unwrap_err();

    assert!(matches!(
        error,
        ConversionError::MissingField {
            record: "TapeFileLsItem",
            field: "tf"
        }
    ));
}

#[test]
fn rejects_invalid_checksum() {
    let item = TapeFileLsItem {
        af: Some(tape_file_ls_item::ArchiveFile {
            checksum: vec![cta_protobuf::cta::admin::Checksum {
                r#type: cta_protobuf::cta::common::checksum_blob::checksum::Type::Adler32.into(),
                value: "not hex".into(),
            }],
            ..Default::default()
        }),
        df: Some(Default::default()),
        tf: Some(Default::default()),
        ..Default::default()
    };

    let error = File::try_from(item).unwrap_err();

    assert!(matches!(
        error,
        ConversionError::InvalidChecksum {
            field: "af.checksum",
            ..
        }
    ));
}

#[test]
fn file_id() {
    assert_eq!(FileId::from(42).to_string(), "42");
    assert_eq!(
        FileId::disk("4301523000", "eosctaatlaspps").to_string(),
        "4301523000@eosctaatlaspps"
    );
}

mod recycle_bin {
    use super::*;

    #[test]
    fn converts_the_records() {
        let files = convert::<RecycleTapeFileLsItem, RecycleBinTapeFile>(cta_admin_jsonl!(
            "recycle_bin_tape_files.jsonl"
        ));

        assert_eq!(files.len(), 3);
        assert_eq!(
            files[0],
            RecycleBinTapeFile {
                file: File {
                    archive_file: ArchiveFile {
                        id: 4_302_486_079,
                        storage_class: "ftstest_IBM1L9".into(),
                        creation_time: ts(1_666_971_586),
                        size: 1_024_000,
                        checksums: vec![Checksum::new(
                            ChecksumType::Adler32,
                            vec![0x11, 0x84, 0xdd, 0xbc]
                        )],
                    },
                    tape_file: TapeFile {
                        vid: "L85033".into(),
                        copy_nb: 1,
                        block_id: 13_000_383,
                        f_seq: 1_000_032,
                    },
                    disk_file: DiskFile {
                        instance: "eosctaatlaspps".into(),
                        id: "4301523000".into(),
                        path: "/eos/ctaatlaspps/ftstest/IBM1L9/ftstest1/1/test01000001".into(),
                        id_when_deleted: Some("4301523000".into()),
                        owner: Some(Owner(89_952, 2766)),
                    },
                },
                deletion_time: ts(1_679_864_202),
                deletion_reason: "File deleted by fts3ops from the eosctaatlaspps instance".into(),
                virtual_organisation: "IT".into(),
            }
        );
        assert_eq!(files[0].key(), (4_302_486_079, "L85033", 1_000_032));
    }
}

mod checksum {
    use super::*;

    mod hex_to_byte_array {
        use super::*;

        #[test]
        fn plain_hex() {
            assert_eq!(hex_to_byte_array("1a2b").unwrap(), [0x1a, 0x2b]);
            assert_eq!(hex_to_byte_array("00").unwrap(), [0x00]);
            assert_eq!(hex_to_byte_array("ff").unwrap(), [0xff]);
        }

        #[test]
        fn both_prefix_spellings() {
            assert_eq!(hex_to_byte_array("0x1a2b").unwrap(), [0x1a, 0x2b]);
            assert_eq!(hex_to_byte_array("0X1a2b").unwrap(), [0x1a, 0x2b]);
        }

        #[test]
        fn odd_digits_left_padded() {
            assert_eq!(hex_to_byte_array("0x1a2").unwrap(), [0x01, 0xa2]);
            assert_eq!(hex_to_byte_array("f").unwrap(), [0x0f]);
            assert_eq!(hex_to_byte_array("abcde").unwrap(), [0x0a, 0xbc, 0xde]);
        }

        #[test]
        fn case_insensitive() {
            assert_eq!(
                hex_to_byte_array("DEADBEEF").unwrap(),
                hex_to_byte_array("deadbeef").unwrap()
            );
        }

        #[test]
        fn empty_input() {
            assert!(hex_to_byte_array("").unwrap().is_empty());
            assert!(hex_to_byte_array("0x").unwrap().is_empty());
        }

        #[test]
        fn rejects_non_hex() {
            assert!(hex_to_byte_array("12zz").is_err());
            assert!(hex_to_byte_array("hello").is_err());
            assert!(hex_to_byte_array("12 34").is_err());
        }

        /// Multi-byte characters used to be chunked on byte boundaries, which fed
        /// invalid UTF-8 into `str::from_utf8().unwrap()` and panicked instead of
        /// returning an error.
        #[test]
        fn rejects_multi_byte_without_panic() {
            for input in ["€", "0x€", "ä", "12€34", "aä", "🦀"] {
                assert!(
                    hex_to_byte_array(input).is_err(),
                    "{input:?} should be rejected"
                );
            }
        }
    }
}
