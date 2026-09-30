// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod cli;
mod cmd;
mod eos;
mod output;
mod parse;

use cern_st_grpc::{EndpointConfig, JwtAuth, validate_scheme};
use clap::Parser;
use eos_client::EosEndpointMap;

/// Dispatches the parsed subcommand.
async fn run_commands(
    config: EndpointConfig,
    args: cli::Cli,
    mut endpoint_map: EosEndpointMap,
) -> anyhow::Result<()> {
    match args.command {
        cli::Command::List {
            json,
            common_options: common,
        } => cmd::list_command(&config, common, json).await,
        cli::Command::Restore(common) => {
            cmd::restore_command(&config, &mut endpoint_map, common).await
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = cli::Cli::parse();

    // Initialize logging. Take RUST_LOG, otherwise CLI option.
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(args.log_level.to_string()),
    )
    .init();

    // Validate frontend endpoint scheme
    if let Err(e) = validate_scheme(&args.cta_frontend_endpoint) {
        eprintln!("Invalid CTA frontend endpoint: {e}");
        std::process::exit(1);
    }

    // get JWT token from file
    let jwt_token = tokio::fs::read(&args.jwt_token_file)
        .await
        .unwrap_or_else(|e| {
            eprintln!(
                "Error reading JWT token file '{}': {e}",
                args.jwt_token_file.to_string_lossy()
            );
            std::process::exit(1);
        });

    // build the actual endpoint config
    let cta_config = EndpointConfig::new(
        args.cta_frontend_endpoint.clone(),
        JwtAuth::new(jwt_token)?,
        args.ca_cert_bundle.as_ref().map(|v| v.into()),
        args.alternative_cta_hostname.clone(),
    );

    // build the EOS endpoint map
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
            std::process::exit(1);
        }
    }
    Ok(())
}
