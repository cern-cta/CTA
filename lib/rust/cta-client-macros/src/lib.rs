// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![forbid(unsafe_code)]

//! Procedural macros of `cta-client`.
//!
//! Use them through `cta-client`, which re-exports them (`cta_client::typed_admin_cmd`): the generated code refers to
//! `::cta_client` paths, so they only work in a crate depending on it (or in `cta-client` itself, which declares
//! `extern crate self as cta_client`).

mod args;
mod expand;
mod field;
mod types;

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

use crate::{args::CmdArgs, expand::expand, field::strip_proto_attrs};

/// Turn a struct into a typed admin command.
///
/// The struct gets:
///
/// * `#[derive(Debug, Clone, PartialEq, Eq)]` and a [`bon`](https://docs.rs/bon) builder, whose setters of `String`
///   fields take `impl Into<String>`;
/// * `impl From<Struct> for AdminCmd`, sending its fields as the options of the command;
/// * `impl TypedAdminCmd`, giving its command and subcommand (`CMD` and `SUBCMD`);
/// * `impl StreamingAdminCmd` with a `response = Record` argument, `impl UnaryAdminCmd` otherwise: whether the command
///   is sent with `AdminClient::stream` or `AdminClient::unary`.
///
/// ```
/// use cta_client::{admin::commands::StreamingAdminCmd, typed_admin_cmd};
/// use cta_protobuf::cta::{admin::{AdminCmd, DriveLsItem}, xrd::data::Data};
///
/// /// Lists drives
/// #[typed_admin_cmd(Drive::Ls, response = DriveLsItem)]
/// #[derive(Default)]
/// pub struct DriveLs {
///     /// Only this drive (the option `Drive`)
///     pub drive: Option<String>,
///     /// Only the drives of this logical library (the option `LogicalLibrary`)
///     #[proto(LogicalLibrary)]
///     pub library: Option<String>,
///     /// Include the drives of disabled logical libraries (the flag `All`)
///     #[proto(only_if_true)]
///     pub all: bool,
/// }
///
/// let cmd: AdminCmd = DriveLs::builder().drive("drive0").all(true).build().into();
/// assert_eq!(DriveLs::item(Data::DrlsItem(DriveLsItem::default())).unwrap(), DriveLsItem::default());
/// ```
///
/// The arguments of the attribute:
///
/// * `Command::SubCommand` (required, first) names the protobuf command and subcommand: the variant of
///   `admin_cmd::Cmd`, and the one of `admin_cmd::SubCmd` without its `Subcmd` prefix (`Tape::Ls` is
///   `Cmd::Tape` and `SubCmd::SubcmdLs`). A command alone (`Version`) has the subcommand `SubcmdNone`.
/// * `response = Record` marks a streaming command, answered with records of the given `cta_protobuf::cta::admin`
///   type (one of the records of the `Data` frames, e.g. `TapeLsItem`).
/// * `with(Key = value, ...)` gives options sent with a fixed value with every command, after the fields:
///   `with(Summary = true)`. The type of a literal value is inferred (`true` is a `bool`, `42` a `u64`, `"text"` a
///   `&str`); another value needs one: `Key: Type = value`.
///
/// ```
/// # use cta_client::typed_admin_cmd;
/// /// Shows the totals of the failed requests
/// #[typed_admin_cmd(Failedrequest::Ls, response = FailedRequestLsSummary, with(Summary = true))]
/// pub struct FailedRequestSummary {}
///
/// /// Shows the versions of the client and the server
/// #[typed_admin_cmd(Version, response = VersionItem)]
/// pub struct Version {}
/// ```
///
/// Every field is sent as an option, through the `OptionValue` implementation of its type (see `admin_cmd!` for the
/// types):
///
/// * its key is the name of the field in `PascalCase` (`tape_pool` is sent as `TapePool`), unless another one is given
///   with `#[proto(Key)]`;
/// * `#[proto(only_if_true)]` (or `#[proto(Key, only_if_true)]`) sends a `bool` only when `true`, and
///   implies `#[builder(default)]`.
///
/// The other attributes of the struct and its fields are kept, as is the visibility of the fields.
///
/// A key is looked up among the options of the type of its field, so a key that does not exist, or belongs to another
/// option list, does not compile:
///
/// ```compile_fail,E0599
/// #[cta_client::typed_admin_cmd(Tape::Ls)]
/// struct Ls {
///     /// `Full` is a boolean option, not a string one
///     pub full: Option<String>,
/// }
/// ```
#[proc_macro_attribute]
pub fn typed_admin_cmd(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as CmdArgs);
    let mut input = parse_macro_input!(input as DeriveInput);
    match expand(&args, input.clone()) {
        Ok(tokens) => tokens.into(),
        Err(error) => {
            // Keep the struct, so that the error is reported alone rather than with every use of the struct.
            strip_proto_attrs(&mut input);
            let error = error.into_compile_error();
            quote!(#input #error).into()
        }
    }
}
