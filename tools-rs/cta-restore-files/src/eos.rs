// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use anyhow::{Result, anyhow};
use std::num::ParseIntError;
use std::path::PathBuf;

use cta_lib::eos::{DEFAULT_FILE_MODE, EosGrpcClient, Error, system_time_now};

use cta_protobuf::cta::admin::RecycleTapeFileLsItem;
use eos_protobuf::eos::rpc::{Checksum, FileMdProto, Time};

const DEFAULT_FILE_LAYOUT: u32 = 0x2 /* Adler */ |
    (0x1 << 4)    /* 1 replica */  |
//  (0x0 << 8)    /* 1 stripe */   |
//  (0x0 << 16)   /* 4K blocks */  |
    (0x1 << 20)   /* block checksum */;

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
        .add_container(&parent_dir.to_string_lossy(), &file.storage_class, true)
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
