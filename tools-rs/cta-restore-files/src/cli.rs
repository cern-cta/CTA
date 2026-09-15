// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Command line interface of `cta-restore-files`.
//!
//! [`Cli`] holds the global options (connection, credentials and the filters
//! that select the recycle-bin entries to act on) and the [`Command`] to run.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use url::Url;

/// The subcommand to execute.
#[derive(Subcommand)]
pub(crate) enum Command {
    /// List the deleted tape files matching the selection options.
    List {
        /// Show results as JSON
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Restore the deleted tape files matching the selection options
    Restore,
}

/// Parsed command line arguments.
#[derive(Parser)]
pub(crate) struct Cli {
    /// The operation to perform.
    #[command(subcommand)]
    pub(crate) command: Command,

    /// gRPC endpoint of the CTA admin frontend service
    #[arg(long)]
    pub(crate) cta_frontend_endpoint: Url,

    /// Path to JWT token file for authentication
    #[arg(long)]
    pub(crate) jwt_token_file: PathBuf,

    /// Path to alternative CA certificate file for TLS connections
    #[arg(long)]
    pub(crate) ca_cert_bundle: Option<PathBuf>,

    /// Alternative CTA server hostname for TLS
    #[arg(long)]
    pub(crate) alternative_cta_hostname: Option<String>,

    /// Path to the namespace keytab file
    #[arg(long, default_value = "namespace.keytab")]
    pub(crate) namespace_keytab_file: PathBuf,

    /// Archive file ID of the file to recover
    #[arg(long)]
    pub(crate) archive_file_id: Option<u64>,

    /// The disk instance to recover from
    #[arg(long)]
    pub(crate) disk_instance: Option<String>,

    /// The File IDs to recover (comma-separated list)
    #[arg(long, name = "file-id", value_delimiter = ',')]
    pub(crate) file_ids: Option<Vec<String>>,

    /// The volume ID to recover from
    #[arg(long)]
    pub(crate) vid: Option<String>,

    /// Copy number of the files to restore
    #[arg(long)]
    pub(crate) copy_number: Option<u64>,
}
