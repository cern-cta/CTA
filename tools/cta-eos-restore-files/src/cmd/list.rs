// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use cta_client::admin::AdminClient;
use cta_grpc_common::EndpointConfig;

use crate::{
    cli::CommonOptions,
    cta,
    output::{output_as_json, output_as_table},
};

pub(crate) async fn command(
    config: &EndpointConfig,
    options: CommonOptions,
    json: bool,
) -> anyhow::Result<()> {
    let client = AdminClient::connect(config).await?;

    // Print header for table output
    if !json {
        println!("Listing deleted files in CTA Catalogue:");
    }

    let files = cta::list_deleted_files(&client, options.recycle_bin_query()).await?;

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
