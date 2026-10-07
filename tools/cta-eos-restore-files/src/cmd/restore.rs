// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Context;
use cern_st_grpc::EndpointConfig;
use cta_client::{
    client::{CtaGrpcClient, StreamingClientType},
    errors::ChecksumTypeError,
    types::{Checksum, ChecksumType, DiskFile, File, FileSelector},
};
use eos_client::EosEndpointMap;
use eos_protobuf::eos::rpc::{MdId, Type};
use tokio_stream::StreamExt;

use crate::{cli::CommonOptions, eos::restore_deleted_file};

async fn do_sanity_check(
    streaming_client: &mut CtaGrpcClient<StreamingClientType>,
    endpoint_map: &mut EosEndpointMap,
    file: File,
) -> anyhow::Result<()> {
    // Let's get the metadata
    let tape_file = streaming_client.get_file(file.archive_file.id).await?;
    let DiskFile {
        id: disk_id,
        instance: disk_instance,
        ..
    } = tape_file.disk_file;

    let eos_metadata = endpoint_map
        .get_file_metadata(
            &disk_instance,
            MdId {
                id: disk_id.parse::<u64>().map_err(|e| {
                    anyhow::anyhow!("disk file ID '{}' is not a valid u64: {}", disk_id, e)
                })?,
                r#type: Type::File.into(),
                ..Default::default()
            },
        )
        .await?;

    log::info!("get_file_metadata: {eos_metadata:#?}");

    let eos_file_id: u64 = std::str::from_utf8(
        eos_metadata
            .xattrs
            .get("sys.archive.file_id")
            .ok_or_else(|| anyhow::anyhow!("EOS metadata missing sys.archive.file_id"))?,
    )
    .map_err(|e| anyhow::anyhow!("sys.archive.file_id is not valid UTF-8: {}", e))?
    .parse()
    .map_err(|e| anyhow::anyhow!("sys.archive.file_id is not a valid u64: {}", e))?;

    // Check that the archive_file_ids match
    if eos_file_id != file.archive_file.id {
        eprintln!(
            "Sanity check: file IDs don't match (eos = {eos_file_id}, cta = {})",
            file.archive_file.id
        );
        std::process::exit(1);
    }

    log::info!("File IDs match ({eos_file_id})!");

    let eos_checksums: anyhow::Result<HashMap<ChecksumType, Checksum>> = eos_metadata
        .checksums
        .into_iter()
        .map(|c| {
            let checksum_type_str = c.r#type.clone();
            let ty = ChecksumType::try_from_eos(c.r#type).map_err(|e| {
                anyhow::anyhow!(
                    "Can't parse EOS checksum type '{}': {}",
                    checksum_type_str,
                    e
                )
            })?;
            Ok((ty, Checksum::new(ty, c.value)))
        })
        .collect();

    let eos_checksums = eos_checksums.context("Failed to parse EOS checksums from metadata")?;

    let cta_checksums: HashMap<ChecksumType, Checksum> = file
        .archive_file
        .checksums
        .iter()
        .map(|cs| -> Result<_, ChecksumTypeError<()>> { Ok((cs.r#type, cs.clone())) })
        .collect::<Result<_, ChecksumTypeError<_>>>()
        .map_err(|e| anyhow::anyhow!("Error parsing CTA checksums: {e:?}"))?;

    let matching: Vec<&ChecksumType> = eos_checksums
        .iter()
        .filter(|(ty, eos_cs)| cta_checksums.get(ty) == Some(eos_cs))
        .map(|(ty, _)| ty)
        .collect();

    // Check that the checksums match
    if matching.is_empty() {
        eprintln!(
            "Sanity check: error matching checksums (eos = {eos_checksums:#?}, cta = {cta_checksums:#?})."
        );
        std::process::exit(1);
    }

    log::info!(
        "Checksums match ({} checked)",
        matching
            .iter()
            .map(|ty| ty.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(())
}

pub(crate) async fn command(
    config: &EndpointConfig,
    endpoint_map: &mut EosEndpointMap,
    common: CommonOptions,
) -> anyhow::Result<()> {
    let mut streaming_client = CtaGrpcClient::new_streaming(config).await?;
    let mut deleted_files = streaming_client
        .list_deleted_files(
            FileSelector::builder()
                .maybe_vid(common.vid)
                .maybe_disk_instance(common.disk_instance)
                .maybe_archive_file_id(common.archive_file_id)
                .maybe_copy_number(common.copy_number)
                .maybe_file_ids(common.file_ids)
                .build(),
        )
        .await?;

    while let Some(file) = deleted_files.next().await {
        let mut file = file?;
        let File {
            disk_file:
                DiskFile {
                    id: ref old_disk_file_id,
                    instance: ref disk_instance,
                    ..
                },
            ..
        } = file;

        let does_file_exist = endpoint_map
            .check_file_exists_by_disk_id(disk_instance, old_disk_file_id)
            .await?;

        if !does_file_exist {
            log::info!("Restoring file '{old_disk_file_id}', which doesn't exist in EOS anymore");
            let mut client = endpoint_map.get_client(&disk_instance).await?;
            let new_disk_file_id = restore_deleted_file(&mut client, &file).await?;
            file.disk_file.id = new_disk_file_id.to_string();
        };

        let mut sync_client = CtaGrpcClient::new_unary(config).await?;
        sync_client.restore_deleted_file_copy(&file).await?;

        log::info!("Running a sanity check on the recovered file...");

        do_sanity_check(&mut streaming_client, endpoint_map, file).await?;
        println!("Sanity check OK.");
    }
    Ok(())
}
