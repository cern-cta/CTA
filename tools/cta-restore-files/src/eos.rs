// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Reconstruction of deleted files in the EOS namespace.
//!
//! CTA keeps enough metadata in its recycle bin to recreate the namespace entry
//! of a deleted file: path, owner, size, checksum and storage class. This
//! module turns such a record back into an EOS file entry — see
//! [`restore_deleted_file`].

use std::{os::unix::ffi::OsStringExt, path::PathBuf};

use anyhow::{Result, anyhow};
use cern_st_grpc::utils::system_time_now;
use cta_client::types::{ChecksumType, File, Owner};
use eos_client::{DEFAULT_FILE_MODE, EosGrpcClient, Error};
use eos_protobuf::eos::rpc::{Checksum, FileMdProto, Time};

/// Layout id assigned to restored files: Adler32 checksum, a single replica,
/// one stripe, 4K blocks and block checksums enabled.
const DEFAULT_FILE_LAYOUT: u32 = 0x2 /* Adler */ |
    (0x1 << 4)    /* 1 replica */  |
//  (0x0 << 8)    /* 1 stripe */   |
//  (0x0 << 16)   /* 4K blocks */  |
    (0x1 << 20)   /* block checksum */;

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
/// * `sys.eos.btime` — the "birth time" of the entry
///
/// # Errors
///
/// Fails if the record does not carry exactly one ADLER32 checksum, if its
/// path has no parent container, or if any of the EOS calls fail.
pub async fn restore_deleted_file(eos: &mut EosGrpcClient, file: &File) -> Result<u64> {
    // let's do some check on the checksums, so that we can quit early if
    // they're not ok.

    // we assume there is a single checksum which is Adler32
    let cs = match file.archive_file.checksums.as_slice() {
        [checksum] => checksum,
        [] => return Err(anyhow!("File '{}' has no checksums", file.disk_file.path)),
        many => {
            return Err(anyhow!(
                "File '{}' has {} checksums, expected exactly one",
                file.disk_file.path,
                many.len()
            ));
        }
    };
    // Only ADLER32 checksums are supported
    match cs.r#type {
        ChecksumType::Adler32 => { /* ok */ }
        other => {
            return Err(Error::UnexpectedReturnValue(format!(
                "Unsupported checksum type: {other:?}"
            ))
            .into());
        }
    }

    // print the EOS ids for information
    let (cur_container_id, cur_file_id) = eos.get_current_ids().await?;
    log::info!("Current EOS ids - container: {cur_container_id} | file: {cur_file_id}");

    match eos.get_file_disk_id_by_path(&file.disk_file.path).await {
        Ok(fid) => {
            log::info!(
                "File '{}' has already got ID '{}'",
                file.disk_file.path,
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

    let path = PathBuf::from(file.disk_file.path.clone());
    let parent_dir = path.parent().ok_or(Error::RootContainerReached)?;

    // First, create the container
    let container_id = eos
        .add_container(
            &parent_dir.to_string_lossy(),
            &file.archive_file.storage_class,
            true,
            None,
        )
        .await?;

    log::debug!("Container ID is '{container_id}'");

    let current_path = PathBuf::from(file.disk_file.path.clone());
    let secs_since_epoch = system_time_now();

    // .unwrap(): recycle-bin entries always carry owner information
    let Owner(uid, gid) = file.disk_file.owner.as_ref().unwrap();

    // Then, create the actual file
    let mut new_file = FileMdProto {
        cont_id: container_id,
        uid: *uid as u64,
        gid: *gid as u64,
        size: file.archive_file.size,
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
        path: current_path.clone().into_os_string().into_vec(),
        name: current_path
            .file_name()
            // .unwrap(): recycle-bin entries have valid file paths with a filename component
            .unwrap()
            .to_string_lossy()
            .to_string()
            .into(),
        ..Default::default()
    };

    let checksum = Checksum {
        r#type: "ADLER32".to_string(),
        value: cs.value.clone(),
    };
    // TODO: remove this once the EOS protobuf API is fixed
    #[expect(deprecated)]
    {
        new_file.checksum = Some(checksum.clone());
    }
    new_file.checksums = vec![checksum];

    // Extended attributes:
    //
    // 1. Archive File ID
    new_file.xattrs.insert(
        "sys.archive.file_id".to_string(),
        file.archive_file.id.to_string().into(),
    );

    // 2. Birth Time
    // POSIX ATIME (Access Time) is used by CTA to store the file creation time.
    // EOS reads the birth time from the `sys.eos.btime` extended attribute
    new_file.xattrs.insert(
        "sys.eos.btime".to_string(),
        secs_since_epoch.to_string().into(),
    );

    if file.archive_file.size > 0 {
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

    let new_id = eos.get_file_disk_id_by_path(&file.disk_file.path).await?;

    log::info!("The new file ID is {}", new_id);

    Ok(new_id)
}
