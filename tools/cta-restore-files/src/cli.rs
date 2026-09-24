// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Command line interface of `cta-restore-files`.
//!
//! [`Cli`] holds the global options (connection, credentials and the filters
//! that select the recycle-bin entries to act on) and the [`Command`] to run.

use std::path::PathBuf;

use clap::{
    Args, Parser, Subcommand,
    builder::{
        Styles,
        styling::{AnsiColor, Color, RgbColor, Style},
    },
};
use url::Url;

const CTA_ORANGE: RgbColor = RgbColor(232, 115, 44); // #E8732C
const CTA_GRAY: RgbColor = RgbColor(148, 152, 158); // #94989E

pub const CLAP_STYLING: Styles = Styles::styled()
    .header(Style::new().fg_color(Some(Color::Rgb(CTA_ORANGE))).bold())
    .usage(Style::new().fg_color(Some(Color::Rgb(CTA_ORANGE))).bold())
    .literal(Style::new().bold())
    .placeholder(Style::new().fg_color(Some(Color::Rgb(CTA_GRAY))))
    .error(AnsiColor::Red.on_default().bold())
    .valid(Style::new().fg_color(Some(Color::Rgb(CTA_ORANGE))))
    .invalid(AnsiColor::Yellow.on_default());

#[derive(Debug, Args)]
#[command(group(
    clap::ArgGroup::new("recovery_selector")
        .multiple(true)
        .required(true)
        .args(["disk_instance", "archive_file_id", "file-id", "vid", "copy_number"])
))]
pub(crate) struct CommandOptions {
    /// The disk instance to recover from
    #[arg(long)]
    pub(crate) disk_instance: Option<String>,

    /// Archive file ID of the file to recover
    #[arg(long)]
    pub(crate) archive_file_id: Option<u64>,

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

/// The subcommand to execute.
#[derive(Subcommand)]
#[command(flatten_help = true)]
pub(crate) enum Command {
    /// List the deleted tape files matching the selection options.
    List {
        /// Show results as JSON
        #[arg(long, default_value_t = false)]
        json: bool,

        #[command(flatten)]
        common_options: CommandOptions,
    },
    /// Restore the deleted tape files matching the selection options
    Restore(CommandOptions),
}

/// Parsed command line arguments.
#[derive(Parser)]
#[command(styles = CLAP_STYLING)]
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
}
