// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Test fixtures shared with the tests of `cta-client` (feature `test-support`): the `cta-admin --jsonl` samples of
//! the `cta-sdk` unit tests, which are also the records of the protocol in proto3 JSON.

use serde::de::DeserializeOwned;

/// Sample `cta-admin --jsonl` output from the `cta-sdk` unit tests, as a `&'static str`.
///
/// The path is resolved from the manifest of the crate using the macro, so it works from the crates of `lib/rust/`.
#[macro_export]
macro_rules! cta_admin_jsonl {
    ($file:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../python/cta-sdk/tests/unit/cta/sdk/daos/cta_admin/data/",
            $file
        ))
    };
}
pub use cta_admin_jsonl;

/// The non-empty lines of a `cta-admin --jsonl` sample, deserialized.
///
/// # Panics
///
/// If a line is not valid JSON for `T`.
#[must_use]
pub fn parse<T: DeserializeOwned>(jsonl: &str) -> Vec<T> {
    jsonl
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid cta-admin JSON"))
        .collect()
}
