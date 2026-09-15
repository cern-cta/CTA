// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![doc = include_str!("../README.md")]
#![feature(gethostname)]
#![feature(iter_array_chunks)]
#![warn(missing_docs)]

mod cli;
mod cta;
mod eos;
mod output;
mod parse;

use std::{fs, net::hostname, process::exit};

use clap::Parser;
use cta_lib::{
    eos::EosEndpointMap,
    rpc::{EndpointConfig, JwtAuth},
};
use cta_protobuf::cta::admin::RecycleTapeFileLsItem;

use crate::{cli::Cli, eos::restore_deleted_file, output::OutputFormat};

/// Dispatches the parsed subcommand.
async fn run_commands(
    config: EndpointConfig,
    args: Cli,
    mut endpoint_map: EosEndpointMap,
) -> anyhow::Result<()> {
    let client = cta::CtaEndpoint { config };

    match args.command {
        cli::Command::List { json } => {
            client
                .list_deleted_files(
                    if json {
                        OutputFormat::Json
                    } else {
                        OutputFormat::Table
                    },
                    args.vid,
                    args.disk_instance,
                    args.archive_file_id,
                    args.copy_number,
                    args.file_ids,
                )
                .await?;
        }
        cli::Command::Restore => {
            // unwrap: it's OK because OutputFormat::None implies a Some(...) return value
            let deleted_files = client
                .list_deleted_files(
                    OutputFormat::None,
                    args.vid,
                    args.disk_instance,
                    args.archive_file_id,
                    args.copy_number,
                    args.file_ids,
                )
                .await?
                .unwrap();

            for mut file in deleted_files {
                let RecycleTapeFileLsItem {
                    disk_instance,
                    disk_file_id,
                    ..
                } = &file;
                let does_file_exist = endpoint_map
                    .check_file_exists_by_disk_id(disk_instance, disk_file_id)
                    .await?;

                if !does_file_exist {
                    log::info!(
                        "Restoring file '{disk_file_id}', which doesn't exist in EOS anymore"
                    );
                    let mut client = endpoint_map.get_client(disk_instance).await?;
                    let new_disk_file_id = restore_deleted_file(&mut client, &file).await?;
                    file.disk_file_id = new_disk_file_id.to_string();
                }

                client.restore_deleted_file_copy(&file).await?;

                // TODO: sanity check
            }
        }
    };

    Ok(())
}

/// Entry point: parses the command line, builds the CTA and EOS endpoint
/// configuration and runs the requested subcommand.
///
/// Exits with status 1 on a usage error (unsupported endpoint scheme,
/// unreadable keytab) or when the subcommand fails.
#[tokio::main]
async fn main() {
    // Initialize logging
    env_logger::init();

    let args = cli::Cli::parse();
    let scheme = args.cta_frontend_endpoint.scheme();

    if !["http", "https"].contains(&scheme) {
        eprintln!("Scheme '{scheme}' is not recognized. Use 'http' or 'https'");
        std::process::exit(1);
    }

    let cta_config = EndpointConfig::new(
        args.cta_frontend_endpoint.clone(),
        JwtAuth::new(fs::read(&args.jwt_token_file).unwrap_or_else(|_| {
            panic!(
                "Can't open file '{}'",
                args.jwt_token_file.to_string_lossy()
            )
        })),
        args.ca_cert_bundle.as_ref().map(|v| v.into()),
        args.alternative_cta_hostname.clone(),
    );

    let _hostname = hostname().unwrap_or("<unknown>".into());

    let endpoint_map = parse::set_namespace_map(
        cta_config.ca_cert_bundle.clone(),
        &args.namespace_keytab_file.to_string_lossy(),
    )
    .unwrap_or_else(|e| {
        eprintln!("Error reading namespace keytab: {}", e);
        std::process::exit(1);
    });

    match run_commands(cta_config, args, endpoint_map).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e}");
            log::error!("{e:?}");
            exit(1);
        }
    }
}
