// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Reconstruction of deleted files in the EOS namespace.
//!
//! CTA keeps enough metadata in its recycle bin to recreate the namespace entry
//! of a deleted file: path, owner, size, checksum and storage class. This
//! module turns such a record back into an EOS file entry — see
//! [`restore_deleted_file`].

use anyhow::{Result, anyhow};
use std::num::ParseIntError;
use std::path::PathBuf;

use cta_lib::eos::{DEFAULT_FILE_MODE, EosGrpcClient, Error, system_time_now};

use cta_protobuf::cta::{admin::RecycleTapeFileLsItem, common::checksum_blob::checksum::Type};
use eos_protobuf::eos::rpc::{Checksum, FileMdProto, Time};

/// Layout id assigned to restored files: Adler32 checksum, a single replica,
/// one stripe, 4K blocks and block checksums enabled.
const DEFAULT_FILE_LAYOUT: u32 = 0x2 /* Adler */ |
    (0x1 << 4)    /* 1 replica */  |
//  (0x0 << 8)    /* 1 stripe */   |
//  (0x0 << 16)   /* 4K blocks */  |
    (0x1 << 20)   /* block checksum */;

/// Decodes a hexadecimal string into its bytes, most significant byte first.
///
/// An optional `0x`/`0X` prefix is accepted and an odd number of digits is
/// treated as if it were left-padded with a zero, so `"0x1a2"` decodes to
/// `[0x01, 0xa2]`. An empty string (or a bare prefix) decodes to an empty
/// vector.
///
/// # Errors
///
/// Returns a [`ParseIntError`] if the input contains anything other than
/// hexadecimal digits, including non-ASCII characters.
pub fn hex_to_byte_array(hex_string: &str) -> Result<Vec<u8>, ParseIntError> {
    let digits = hex_string
        .strip_prefix("0x")
        .or_else(|| hex_string.strip_prefix("0X"))
        .unwrap_or(hex_string);

    // Reject anything that is not an ASCII hex digit up front
    if let Some(invalid) = digits.chars().find(|c| !c.is_ascii_hexdigit()) {
        let mut buffer = [0u8; 4];
        return Err(u8::from_str_radix(invalid.encode_utf8(&mut buffer), 16)
            .expect_err("a non-hexadecimal character must fail to parse"));
    }

    // From here on every character is one ASCII byte, so byte offsets and
    // character offsets coincide.
    let mut bytes = Vec::with_capacity(digits.len().div_ceil(2));
    let mut offset = 0;

    // An odd number of digits means the leading nibble stands alone.
    if digits.len() % 2 == 1 {
        bytes.push(u8::from_str_radix(&digits[..1], 16)?);
        offset = 1;
    }

    while offset < digits.len() {
        bytes.push(u8::from_str_radix(&digits[offset..offset + 2], 16)?);
        offset += 2;
    }

    Ok(bytes)
}

/// Restore a file which has been deleted, within the EOS namespace.
///
/// Returns the EOS file id (disk file id) of the restored entry. The operation
/// is idempotent: if a file already exists at the recorded path, its id is
/// returned without touching the namespace.
///
/// Otherwise the parent containers are created as needed (inheriting the
/// record's storage class) and a file entry is inserted with the owner, size,
/// [`DEFAULT_FILE_LAYOUT`], [`DEFAULT_FILE_MODE`] and checksum from the
/// recycle-bin record, plus these extended attributes:
///
/// * `sys.archive.file_id` — the CTA archive file id, used to retrieve the
///   file from tape;
/// * `eos.btime` — the "birth time" of the entry
///
/// # Errors
///
/// Fails if the record does not carry exactly one ADLER32 checksum, if its
/// path has no parent container, or if any of the EOS calls fail.
pub async fn restore_deleted_file(
    eos: &mut EosGrpcClient,
    file: &RecycleTapeFileLsItem,
) -> Result<u64> {
    // let's do some check on the checksums, so that we can quit early if
    // they're not ok.

    // we assume there is a single checksum which is Adler32
    let cs = match file.checksum.as_slice() {
        [checksum] => checksum,
        [] => return Err(anyhow!("File '{}' has no checksums", file.disk_file_path)),
        many => {
            return Err(anyhow!(
                "File '{}' has {} checksums, expected exactly one",
                file.disk_file_path,
                many.len()
            ));
        }
    };
    // Only ADLER32 checksums are supported
    match cs.r#type() {
        Type::Adler32 => { /* ok */ }
        other => {
            return Err(Error::UnexpectedReturnValue(format!(
                "Unsupported checksum type: {other:?}"
            ))
            .into());
        }
    }

    let eos_ids = eos.get_current_ids().await?;
    println!("{eos_ids:?}");

    match eos.get_file_disk_id_by_path(&file.disk_file_path).await {
        Ok(fid) => {
            log::info!(
                "File '{}' has already got ID '{}'",
                file.disk_file_path,
                fid
            );
            // File was restored in the meantime. Do nothing.
            return Ok(fid);
        }
        Err(Error::NotFound(_)) => {
            // File not found -> continue
        }
        Err(e) => return Err(e.into()),
    }

    let path = PathBuf::from(file.disk_file_path.clone());
    let parent_dir = path.parent().ok_or(Error::RootContainerReached)?;

    // First, create the container
    let container_id = eos
        .add_container(
            &parent_dir.to_string_lossy(),
            &file.storage_class,
            true,
            None,
        )
        .await?;

    log::debug!("Container ID is '{container_id}'");

    let current_path = PathBuf::from(file.disk_file_path.clone());
    let secs_since_epoch = system_time_now();

    // Then, create the actual file
    let mut new_file = FileMdProto {
        cont_id: container_id,
        uid: file.disk_file_uid,
        gid: file.disk_file_gid,
        size: file.size_in_bytes,
        layout_id: DEFAULT_FILE_LAYOUT,
        flags: DEFAULT_FILE_MODE.bits(),
        ctime: Some(Time {
            sec: secs_since_epoch,
            n_sec: 0,
        }),
        mtime: Some(Time {
            sec: secs_since_epoch,
            n_sec: 0,
        }),
        path: current_path.to_string_lossy().to_string().into(),
        name: current_path
            .file_name()
            // unwrap: there should always be a name because this is a deleted file, which
            // we assume will have a valid path
            .unwrap()
            .to_string_lossy()
            .to_string()
            .into(),
        ..Default::default()
    };

    new_file.checksums = vec![Checksum {
        r#type: "ADLER32".to_string(),
        value: hex_to_byte_array(&cs.value).map_err(|e| Error::ParseInt(cs.value.clone(), e))?,
    }];

    // Extended attributes:
    //
    // 1. Archive File ID
    new_file.xattrs.insert(
        "sys.archive.file_id".to_string(),
        file.archive_file_id.to_string().into(),
    );

    // 2. Birth Time
    // POSIX ATIME (Access Time) is used by CTA to store the file creation time. EOS calls this "birth time",
    // but there is no place in the namespace to store it, so it is stored as an extended attribute.
    new_file
        .xattrs
        .insert("eos.btime".to_string(), secs_since_epoch.to_string().into());

    if file.size_in_bytes > 0 {
        // Indicate that there is a tape-resident replica of this file (except for zero-length files)
        new_file.locations.push(u16::MAX as u32);
    }

    let reply = eos.insert_files(&[new_file]).await?;

    log::debug!("InsertReply: {reply:#?}");

    log::info!(
        "File '{}' successfully restored in the EOS namespace",
        current_path.to_string_lossy()
    );
    log::debug!("Querying EOS for the new EOS file id...");

    let new_id = eos.get_file_disk_id_by_path(&file.disk_file_path).await?;

    log::info!("The new file ID is {}", new_id);

    Ok(new_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_plain_hex() {
        assert_eq!(hex_to_byte_array("1a2b").unwrap(), [0x1a, 0x2b]);
        assert_eq!(hex_to_byte_array("00").unwrap(), [0x00]);
        assert_eq!(hex_to_byte_array("ff").unwrap(), [0xff]);
    }

    #[test]
    fn accepts_both_prefix_spellings() {
        assert_eq!(hex_to_byte_array("0x1a2b").unwrap(), [0x1a, 0x2b]);
        assert_eq!(hex_to_byte_array("0X1a2b").unwrap(), [0x1a, 0x2b]);
    }

    #[test]
    fn left_pads_an_odd_number_of_digits() {
        assert_eq!(hex_to_byte_array("0x1a2").unwrap(), [0x01, 0xa2]);
        assert_eq!(hex_to_byte_array("f").unwrap(), [0x0f]);
        assert_eq!(hex_to_byte_array("abcde").unwrap(), [0x0a, 0xbc, 0xde]);
    }

    #[test]
    fn is_case_insensitive() {
        assert_eq!(
            hex_to_byte_array("DEADBEEF").unwrap(),
            hex_to_byte_array("deadbeef").unwrap()
        );
    }

    #[test]
    fn decodes_an_empty_input_to_no_bytes() {
        assert!(hex_to_byte_array("").unwrap().is_empty());
        assert!(hex_to_byte_array("0x").unwrap().is_empty());
    }

    #[test]
    fn rejects_non_hexadecimal_digits() {
        assert!(hex_to_byte_array("12zz").is_err());
        assert!(hex_to_byte_array("hello").is_err());
        assert!(hex_to_byte_array("12 34").is_err());
    }

    /// Multi-byte characters used to be chunked on byte boundaries, which fed
    /// invalid UTF-8 into `str::from_utf8().unwrap()` and panicked instead of
    /// returning an error.
    #[test]
    fn rejects_multi_byte_characters_without_panicking() {
        for input in ["€", "0x€", "ä", "12€34", "aä", "🦀"] {
            assert!(
                hex_to_byte_array(input).is_err(),
                "{input:?} should be rejected"
            );
        }
    }
}
