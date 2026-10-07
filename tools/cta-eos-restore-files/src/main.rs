// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![forbid(unsafe_code)]
#![feature(deref_patterns)]
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod cli;
mod cmd;
mod env;
mod eos;
mod output;
mod parse;

use cern_st_grpc::{EndpointConfig, JwtAuth, validate_scheme};
use clap::Parser;

use crate::cli::FilesCli;

/// Initializes the logging system based on the provided CLI log level and a default level.
/// Precedence: CLI option > Env Vars (e.g. `RUST_LOG`) > default level
fn init_logging(cli_level: Option<log::LevelFilter>, default_level: log::LevelFilter) {
    let mut builder = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(default_level.to_string()),
    );
    if let Some(level) = cli_level {
        builder.filter_level(level);
    }
    builder.init();
}

/// Dispatches the parsed subcommand.
async fn run_commands(args: cli::Cli) -> anyhow::Result<()> {
    match args {
        cli::Cli::Commands(FilesCli { config, command }) => {
            // Validate frontend endpoint scheme
            if let Err(e) = validate_scheme(&config.cta_frontend_endpoint) {
                eprintln!("Invalid CTA frontend endpoint: {e}");
                std::process::exit(1);
            }

            // get JWT token from file
            let jwt_token = tokio::fs::read(&config.jwt_token_file)
                .await
                .unwrap_or_else(|e| {
                    eprintln!(
                        "Error reading JWT token file '{}': {e}",
                        config.jwt_token_file.to_string_lossy()
                    );
                    std::process::exit(1);
                });

            // build the actual endpoint config
            let cta_config = EndpointConfig::new(
                config.cta_frontend_endpoint.clone(),
                JwtAuth::new(jwt_token)?,
                config.ca_cert_bundle.as_ref().map(|v| v.into()),
                config.alternative_cta_hostname.clone(),
            );

            // build the EOS endpoint map
            let mut endpoint_map = parse::set_namespace_map(
                cta_config.ca_cert_bundle.clone(),
                &config.namespace_keytab_file.to_string_lossy(),
            )
            .unwrap_or_else(|e| {
                eprintln!("Error reading namespace keytab: {}", e);
                std::process::exit(1);
            });

            match command {
                cli::FilesCommand::List {
                    json,
                    common_options,
                    ..
                } => {
                    // no logging by default for list command, as it is intended to be used in scripts and pipelines
                    init_logging(common_options.log_level, log::LevelFilter::Off);

                    cmd::list_command(&cta_config, common_options, json).await
                }
                cli::FilesCommand::Restore(common_options) => {
                    // default logging level for restore command is INFO, as it is intended to be audited by default
                    init_logging(common_options.log_level, log::LevelFilter::Info);

                    cmd::restore_command(&cta_config, &mut endpoint_map, common_options).await
                }
            }
        }
        cli::Cli::Env(env) => match env.command {
            cli::EnvCommand::GenManPages { out_dir } => env::gen_man_pages(&out_dir),
            cli::EnvCommand::GenCompletion { shell } => env::gen_completion(shell),
        },
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut os_args = std::env::args_os().collect::<Vec<_>>();

    if let Some(bin_name) = std::env::var_os("OVERRIDE_BINARY_NAME") {
        os_args[0] = bin_name;
    }

    let args = cli::Cli::parse_from(os_args);

    match run_commands(args).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e}");
            log::error!("{e:?}");
            std::process::exit(1);
        }
    }

    Ok(())
}
