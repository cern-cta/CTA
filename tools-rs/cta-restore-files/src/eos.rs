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

use cta_protobuf::cta::admin::RecycleTapeFileLsItem;
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
/// left-padded with a zero, so `"0x1a2"` decodes to `[0x01, 0xa2]`. This is
/// needed because CTA stores checksums as hex strings while EOS expects raw
/// bytes.
///
/// # Errors
///
/// Returns a [`ParseIntError`] if the input contains non-hexadecimal
/// characters.
pub fn hex_to_byte_array(hex_string: &str) -> Result<Vec<u8>, ParseIntError> {
    let mut hex_string = hex_string.to_string();

    if hex_string.starts_with("0x") || hex_string.starts_with("0X") {
        hex_string.drain(0..2);
    }

    if hex_string.len() % 2 == 1 {
        hex_string.insert(0, '0');
    }

    hex_string
        .into_bytes()
        .into_iter()
        .array_chunks::<2>()
        .map(|v| u8::from_str_radix(std::str::from_utf8(&v).unwrap(), 16))
        .collect()
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
    (file.checksum.len() == 1).ok_or(anyhow!("File should have one and only one checksum!"))?;

    let cs = file.checksum.first().unwrap();
    (cs.r#type().as_str_name() == "ADLER32").ok_or(Error::UnexpectedReturnValue(
        "Only ADLER32 checksums are supported".into(),
    ))?;

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
