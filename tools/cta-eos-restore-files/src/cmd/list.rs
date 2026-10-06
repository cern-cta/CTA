// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use cern_st_grpc::EndpointConfig;
use cta_client::{client::CtaGrpcClient, types::FileSelector};

use crate::{
    cli::CommonOptions,
    output::{output_as_json, output_as_table},
};

pub(crate) async fn command(
    config: &EndpointConfig,
    options: CommonOptions,
    json: bool,
) -> anyhow::Result<()> {
    let mut client = CtaGrpcClient::new_streaming(config).await?;

    // Print header for table output
    if !json {
        println!("Listing deleted files in CTA Catalogue:");
    }

    let files = client
        .list_deleted_files(
            FileSelector::builder()
                .maybe_vid(options.vid)
                .maybe_disk_instance(options.disk_instance)
                .maybe_archive_file_id(options.archive_file_id)
                .maybe_copy_number(options.copy_number)
                .maybe_file_ids(options.file_ids)
                .build(),
        )
        .await?;

    // Process stream based on output format (collection vs rendering)
    if json {
        // Stream items to JSON output
        output_as_json(files).await?;
    } else {
        // Stream items to table output
        output_as_table(files).await?;
    }
    Ok(())
}
