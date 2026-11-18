// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![forbid(unsafe_code)]

//! Procedural macros of `cta-model`.
//!
//! Use them through `cta-model`, which re-exports them (`cta_model::Keyed`): the generated code refers to
//! `::cta_model` paths, so they only work in a crate depending on it (or in `cta-model` itself, which declares
//! `extern crate self as cta_model`).

mod keyed;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derive `Keyed` from the fields listed by `#[key(...)]`, in that order.
///
/// The key can contain a single field, a tuple of them otherwise. Each field is borrowed according to its
/// type: a `String` as a `&str`, an `Option<String>` as an `Option<&str>`, and any other type is copied (so it must
/// be `Copy`, as the enums of `cta-model` are).
///
/// ```
/// use cta_model::Keyed;
///
/// #[derive(Keyed)]
/// #[key(user_name, disk_instance_name, copy_number)]
/// struct Rule {
///     user_name: String,
///     disk_instance_name: String,
///     copy_number: u32,
///     comment: String,
/// }
///
/// let rule = Rule {
///     user_name: "alice".into(),
///     disk_instance_name: "eosdev".into(),
///     copy_number: 1,
///     comment: String::new(),
/// };
/// assert_eq!(rule.key(), ("alice", "eosdev", 1));
/// ```
#[proc_macro_derive(Keyed, attributes(key))]
pub fn derive_keyed(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    keyed::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
