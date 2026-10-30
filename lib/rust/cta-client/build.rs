// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Build script: sets the versions sent with admin commands, the same way the CTA packaging builds do.
//!
//! * `CTA_CLIENT_VERSION`: `CTA_VERSION` when set (as passed to `CMake`), otherwise the local development version of
//!   as calculated by `ci/utils/cta_version.py` (`<majorVersion>.dev1`, with `majorVersion` read from `project.json`).
//! * `CTA_CLIENT_PROTOBUF_TAG`: `XROOTD_SSI_PROTOBUF_INTERFACE_VERSION` when set (as passed to `CMake`), otherwise
//!   the latest tag of the `xrootd-ssi-protobuf-interface` submodule (`git describe --tags --abbrev=0`).

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn Error>> {
    let repository_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("../../..");
    println!(
        "cargo:rustc-env=CTA_CLIENT_VERSION={}",
        cta_version(&repository_path)?
    );
    println!(
        "cargo:rustc-env=CTA_CLIENT_PROTOBUF_TAG={}",
        protobuf_tag(&repository_path)?
    );
    Ok(())
}

/// Get an env var while telling cargo to keep track of it
fn env_get_and_watch(name: &str) -> Option<String> {
    // tell cargo to rebuild the crate if the env var changes
    println!("cargo:rerun-if-env-changed={name}");
    // The value of an environment variable, if set and not empty
    env::var(name).ok().filter(|value| !value.is_empty())
}

// Get the current CTA version
fn cta_version(repository: &Path) -> Result<String, Box<dyn Error>> {
    // by default, get it from the `CTA_VERSION` env var
    if let Some(version) = env_get_and_watch("CTA_VERSION") {
        return Ok(version);
    }

    // we will retrieve the major version from the `project.json` file
    let project_file = repository.join("project.json");

    // tell cargo to rebuild the crate if `project.json` changes
    println!("cargo:rerun-if-changed={}", project_file.display());

    let project: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&project_file).map_err(|e| {
            format!(
                "CTA_VERSION is not set and {} cannot be read: {e}",
                project_file.display()
            )
        })?)?;

    // get the actual major version
    let major = project["majorVersion"]
        .as_u64()
        .filter(|&major| major > 0)
        .ok_or_else(|| {
            format!(
                "majorVersion must be a positive integer in {}",
                project_file.display()
            )
        })?;

    Ok(format!("{major}.dev1"))
}

/// Get the protobuf interface version
fn protobuf_tag(repository_path: &Path) -> Result<String, Box<dyn Error>> {
    // by default, get it from `XROOTD_SSI_PROTOBUF_INTERFACE_VERSION`
    if let Some(tag) = env_get_and_watch("XROOTD_SSI_PROTOBUF_INTERFACE_VERSION") {
        return Ok(tag);
    }

    // otherwise, let's get it from the protobuf interface submodule version
    let submodule_path =
        repository_path.join("lib/protobuf/external/xrootd-ssi-protobuf-interface");

    // helper closure to run `git`
    let git = |args: &[&str]| -> Result<String, Box<dyn Error>> {
        let output = Command::new("git")
            .arg("-C")
            .arg(&submodule_path)
            .args(args)
            .output()?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).trim().into());
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    };

    // the tag comes from git
    let tag = git(&["describe", "--tags", "--abbrev=0"]).map_err(|e| {
        format!(
            "XROOTD_SSI_PROTOBUF_INTERFACE_VERSION is not set and the tag of {} cannot be found \
             (run 'git submodule update --init'): {e}",
            submodule_path.display()
        )
    })?;

    // tell cargo to rebuild also whenever the submodule moves to another commit
    let git_dir = git(&["rev-parse", "--absolute-git-dir"])?;
    println!("cargo:rerun-if-changed={git_dir}/HEAD");
    Ok(tag)
}
