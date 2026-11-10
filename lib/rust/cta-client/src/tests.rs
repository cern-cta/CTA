// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::types::{File, hex_to_byte_array};

mod file {
    use cta_protobuf::cta::admin::{RecycleTapeFileLsItem, TapeFileLsItem, tape_file_ls_item};

    use super::*;
    use crate::errors::TapeFileLsItemConversionError;

    #[test]
    fn from_tape_file_ls_item() {
        let item = TapeFileLsItem {
            af: Some(tape_file_ls_item::ArchiveFile {
                archive_id: 42,
                ..Default::default()
            }),
            df: Some(tape_file_ls_item::DiskFile {
                disk_id: "7".into(),
                ..Default::default()
            }),
            tf: Some(tape_file_ls_item::TapeFile {
                vid: "V01001".into(),
                copy_nb: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        let file = File::try_from(item).unwrap();

        assert_eq!(file.archive_file.id, 42);
        assert_eq!(file.disk_file.id, "7");
        assert_eq!(file.tape_file.vid, "V01001");
        assert!(file.disk_file.id_when_deleted.is_none());
    }

    #[test]
    fn rejects_incomplete_tape_file_ls_item() {
        let item = TapeFileLsItem {
            af: Some(Default::default()),
            df: Some(Default::default()),
            tf: None,
            ..Default::default()
        };

        let error = File::try_from(item).unwrap_err();

        assert!(matches!(error, TapeFileLsItemConversionError(ref field) if field == "tape_file"));
    }

    #[test]
    fn from_recycle_tape_file_ls_item() {
        let item = RecycleTapeFileLsItem {
            vid: "L85033".into(),
            archive_file_id: 7,
            disk_file_id: "9".into(),
            disk_file_id_when_deleted: "8".into(),
            ..Default::default()
        };

        let file = File::try_from(item).unwrap();

        assert_eq!(file.tape_file.vid, "L85033");
        assert_eq!(file.archive_file.id, 7);
        assert_eq!(file.disk_file.id, "9");
        assert_eq!(file.disk_file.id_when_deleted.as_deref(), Some("8"));
    }
}

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
