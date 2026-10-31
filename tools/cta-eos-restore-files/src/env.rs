// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Environment-related utilities for the CTA restore files tool.

use std::fs;

use clap::CommandFactory;
use clap_complete::{Shell, generate};
use clap_mangen::Man;

const NAME: &str = env!("CARGO_PKG_NAME");

fn render_page(
    cmd: &clap::Command,
    file_stem: &str,
    out_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let mut buffer: Vec<u8> = Default::default();
    Man::new(cmd.clone()).render(&mut buffer)?;
    fs::write(out_dir.join(format!("{file_stem}.1")), buffer)?;
    Ok(())
}

pub(crate) fn gen_man_pages(out_dir: &std::path::Path) -> anyhow::Result<()> {
    let cli = crate::cli::Cli::command();
    // The CLI is defined with the main subcommand at build time, so this cannot fail.
    let applet = cli
        .find_subcommand(NAME)
        .unwrap()
        .clone()
        .display_name(NAME);

    fs::create_dir_all(out_dir)?;

    // Top-level page: cta-eos-restore-files.1
    render_page(&applet, NAME, out_dir)?;

    // One page per subcommand
    for sub in applet.get_subcommands() {
        let full_name = format!("{NAME}-{}", sub.get_name());
        let page = sub
            .clone()
            .display_name(full_name.clone())
            .disable_help_subcommand(true);
        render_page(&page, &full_name, out_dir)?;
    }

    Ok(())
}

pub(crate) fn gen_completion(shell: clap_complete::Shell) -> anyhow::Result<()> {
    let mut cli = crate::cli::Cli::command();
    // The CLI is defined with the main subcommand at build time, so this cannot fail.
    let cmd = cli.find_subcommand_mut(NAME).unwrap();
    let file_name = match shell {
        Shell::Bash => format!("{NAME}.bash"),
        Shell::Zsh => format!("{NAME}.zsh"),
        Shell::Fish => format!("{NAME}.fish"),
        Shell::PowerShell => format!("{NAME}.ps1"),
        Shell::Elvish => format!("{NAME}.elv"),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported shell for completion script generation"
            ));
        }
    };

    let mut file = fs::File::create(&file_name)?;
    generate(shell, cmd, NAME, &mut file);
    println!("Generated completion script: {}", file_name);
    Ok(())
}
