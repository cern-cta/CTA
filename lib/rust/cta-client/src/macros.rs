// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Helper macros for building admin command protobuf messages.
//!
//! The main macro is `admin_cmd!`, which builds an
//! [`cta_protobuf::cta::admin::AdminCmd`] message with typed parameter setters.

/// Build an [`AdminCmd`] message with typed parameter setters.
///
/// The macro mirrors the protobuf structure of a CTA admin command:
/// `Command.SubCommand { Field: Type => value, ... }`.
///
/// # Syntax
///
/// ```text
/// admin_cmd!(Command.SubCommand {
///     FieldName: Type   => expr,        // required — expr must be the value type
///     FieldName: Type?  => option_expr, // optional — expr must be `Option<T>`
/// })
/// ```
///
/// Supported field types:
/// * `str` — a string value, inserted via [`OptionString`]
/// * `u64` — an unsigned integer, inserted via [`OptionUInt64`]
/// * `str_list` — a list of strings, inserted via [`OptionStrList`]
///
/// # Example — required fields only
///
/// Query a specific tape file by its archive ID:
///
/// ```no_run
/// # use cta_client::admin_cmd;
/// let cmd = admin_cmd!(Tapefile.SubcmdLs {
///     ArchiveFileId: u64 => 4294967296u64,
/// });
/// ```
///
/// [`AdminCmd`]: cta_protobuf::cta::admin::AdminCmd
/// [`OptionString`]: cta_protobuf::cta::admin::OptionString
/// [`OptionUInt64`]: cta_protobuf::cta::admin::OptionUInt64
/// [`OptionStrList`]: cta_protobuf::cta::admin::OptionStrList
#[macro_export]
macro_rules! admin_cmd {
    // Entry point
    ($cmd:ident . $sub_cmd:ident { $($fields:tt)* }) => {{
        #[allow(unused_imports, reason = "macro expansion is data-dependent")]
        use cta_protobuf::cta::admin::{
            AdminCmd, OptionString, OptionUInt64, OptionStrList,
            option_string::Key as StringKeys,
            option_u_int64::Key as U64Keys,
            option_str_list::Key as StrListKeys,
            admin_cmd
        };
        #[allow(unused_mut, reason = "macro expansion is data-dependent")]
        let mut cmd = AdminCmd {
            cmd: admin_cmd::Cmd::$cmd.into(),
            subcmd: admin_cmd::SubCmd::$sub_cmd.into(),
            ..Default::default()
        };
        admin_cmd!(@fields cmd, $($fields)*);
        cmd
    }};

    // base case: no fields left
    (@fields $cmd:ident, ) => {};

    // optional field: handle `?`
    (@fields $cmd:ident, $key:ident : $type:ident ? => $val:expr $(, $($rest:tt)*)?) => {
        if let Some(val) = $val {
            admin_cmd!(@field $cmd, $type, $key, val);
        }
        admin_cmd!(@fields $cmd, $($($rest)*)?);
    };

    // required field: no `?`
    (@fields $cmd:ident, $key:ident : $type:ident => $val:expr $(, $($rest:tt)*)?) => {
        admin_cmd!(@field $cmd, $type, $key, $val);
        admin_cmd!(@fields $cmd, $($($rest)*)?);
    };

    // final types
    (@field $cmd:ident, str, $key:ident, $val:expr) => {
        $cmd.option_str.push(OptionString {
            key: StringKeys::$key.into(),
            value: $val.to_string(),
        });
    };
    (@field $cmd:ident, u64, $key:ident, $val:expr) => {
        $cmd.option_uint64.push(OptionUInt64 {
            key: U64Keys::$key.into(),
            value: $val.into(),
        });
    };
    (@field $cmd:ident, str_list, $key:ident, $val:expr) => {
        $cmd.option_str_list.push(OptionStrList {
            key: StrListKeys::$key.into(),
            item: $val.into_iter().map(|s| s.to_string()).collect(),
        });
    };
}
