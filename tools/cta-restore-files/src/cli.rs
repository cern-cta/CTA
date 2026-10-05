// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Command line interface of cta-restore-files.
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
pub(crate) struct CommonOptions {
    #[arg(long, short)]
    pub(crate) log_level: Option<log::LevelFilter>,

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

#[derive(Debug, Args)]
pub(crate) struct ConfigOptions {
    /// gRPC endpoint of the CTA admin frontend service
    #[arg(long, env)]
    pub(crate) cta_frontend_endpoint: Url,

    /// Path to JWT token file for authentication
    #[arg(long, env)]
    pub(crate) jwt_token_file: PathBuf,

    /// Path to alternative CA certificate file for TLS connections
    #[arg(long, env)]
    pub(crate) ca_cert_bundle: Option<PathBuf>,

    /// Alternative CTA server hostname for TLS
    #[arg(long, env)]
    pub(crate) alternative_cta_hostname: Option<String>,

    /// Path to the namespace keytab file
    #[arg(long, env, default_value = "namespace.keytab")]
    pub(crate) namespace_keytab_file: PathBuf,
}

/// `cta-restore-files`: restore deleted tape files from the CTA catalogue and EOS storage
#[derive(Args)]
#[command(flatten_help = true)]
pub(crate) struct FilesCli {
    #[command(flatten)]
    pub(crate) config: ConfigOptions,

    #[command(subcommand)]
    pub(crate) command: FilesCommand,
}

/// The subcommand to execute.
#[derive(Subcommand)]
#[command(flatten_help = true)]
pub(crate) enum FilesCommand {
    /// List the deleted tape files matching the selection options.
    List {
        /// Show results as JSON
        #[arg(long, default_value_t = false)]
        json: bool,

        #[command(flatten)]
        common_options: CommonOptions,
    },
    /// Restore the deleted tape files matching the selection options
    Restore(CommonOptions),
}

/// `cta-restore-files-env`: helper command for generating documentation and completion scripts.
#[derive(Args)]
pub(crate) struct EnvCli {
    #[command(subcommand)]
    pub(crate) command: EnvCommand,
}

#[derive(Subcommand)]
pub(crate) enum EnvCommand {
    /// Generate man pages for the CLI
    GenManPages { out_dir: PathBuf },
    /// Generate shell completion scripts for the CLI
    GenCompletion { shell: clap_complete::Shell },
}

#[derive(Parser)]
#[command(multicall = true)]
#[command(styles = CLAP_STYLING)]
pub(crate) enum Cli {
    #[command(name = "cta-restore-files")]
    Commands(Box<FilesCli>),

    #[command(name = "cta-restore-files-env")]
    Env(EnvCli),
}
