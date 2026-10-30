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
/// * `bool` — a boolean, inserted via [`OptionBoolean`]
/// * `flag` — a `bool` flag, inserted via [`OptionBoolean`] only when `true`
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
/// [`OptionBoolean`]: cta_protobuf::cta::admin::OptionBoolean
#[macro_export]
macro_rules! admin_cmd {
    // Entry point
    ($cmd:ident . $sub_cmd:ident { $($fields:tt)* }) => {{
        #[allow(unused_imports, reason = "macro expansion is data-dependent")]
        use cta_protobuf::cta::admin::{
            AdminCmd, OptionBoolean, OptionString, OptionUInt64, OptionStrList,
            option_boolean::Key as BoolKeys,
            option_string::Key as StringKeys,
            option_u_int64::Key as U64Keys,
            option_str_list::Key as StrListKeys,
            admin_cmd
        };
        #[allow(unused_mut, reason = "macro expansion is data-dependent")]
        let mut cmd = AdminCmd {
            cmd: admin_cmd::Cmd::$cmd.into(),
            subcmd: admin_cmd::SubCmd::$sub_cmd.into(),
            client_version: $crate::commands::CLIENT_VERSION.to_string(),
            protobuf_tag: $crate::commands::PROTOBUF_TAG.to_string(),
            ..Default::default()
        };
        $crate::admin_cmd!(@fields cmd, $($fields)*);
        cmd
    }};

    // base case: no fields left
    (@fields $cmd:ident, ) => {};

    // optional field: handle `?`
    (@fields $cmd:ident, $key:ident : $type:ident ? => $val:expr $(, $($rest:tt)*)?) => {
        if let Some(val) = $val {
            $crate::admin_cmd!(@field $cmd, $type, $key, val);
        }
        $crate::admin_cmd!(@fields $cmd, $($($rest)*)?);
    };

    // required field: no `?`
    (@fields $cmd:ident, $key:ident : $type:ident => $val:expr $(, $($rest:tt)*)?) => {
        $crate::admin_cmd!(@field $cmd, $type, $key, $val);
        $crate::admin_cmd!(@fields $cmd, $($($rest)*)?);
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
    (@field $cmd:ident, bool, $key:ident, $val:expr) => {
        $cmd.option_bool.push(OptionBoolean {
            key: BoolKeys::$key.into(),
            value: $val,
        });
    };
    (@field $cmd:ident, flag, $key:ident, $val:expr) => {
        if $val {
            $crate::admin_cmd!(@field $cmd, bool, $key, true);
        }
    };
}

/// Define a typed admin command: a struct with a [`bon`] builder, its conversion into an [`AdminCmd`] and its
/// [`TypedAdminCmd`](crate::commands::TypedAdminCmd) implementation.
///
/// ```
/// # use cta_client::define_typed_admin_cmd;
/// use cta_protobuf::cta::admin::AdminCmd;
///
/// define_typed_admin_cmd! {
///     /// Lists drives
///     #[proto(Drive.SubcmdLs)]
///     pub struct DriveLs {
///         /// Drive name
///         drive: str? => Drive,
///         /// Include drives of disabled logical libraries
///         all: flag => All,
///     }
/// }
///
/// let cmd: AdminCmd = DriveLs::builder().all(true).build().into();
/// ```
///
/// `#[proto(Command.SubCommand)]` names the protobuf command and subcommand. It is consumed by the macro, may appear
/// anywhere among the attributes of the struct, and is required:
///
/// ```compile_fail
/// # use cta_client::define_typed_admin_cmd;
/// define_typed_admin_cmd! {
///     pub struct DriveLs {
///         all: flag => All,
///     }
/// }
/// ```
///
/// [`AdminCmd`]: cta_protobuf::cta::admin::AdminCmd
/// [`bon`]: https://docs.rs/bon
#[macro_export]
macro_rules! define_typed_admin_cmd {
    // struct attributes: take `#[proto(Command.SubCommand)]` out, keep the others
    (@attrs [$($meta:tt)*] [] #[proto($cmd:ident . $sub_cmd:ident)] $($rest:tt)*) => {
        $crate::define_typed_admin_cmd!(@attrs [$($meta)*] [$cmd $sub_cmd] $($rest)*);
    };
    (@attrs [$($meta:tt)*] [$($proto:tt)*] #[$m:meta] $($rest:tt)*) => {
        $crate::define_typed_admin_cmd!(@attrs [$($meta)* #[$m]] [$($proto)*] $($rest)*);
    };
    (@attrs [$($meta:tt)*] [$cmd:ident $sub_cmd:ident] $vis:vis struct $name:ident { $($fields:tt)* }) => {
        $crate::define_typed_admin_cmd!(@munch [$($meta)* $vis $name $cmd $sub_cmd] [] [] $($fields)*);
    };
    (@attrs [$($meta:tt)*] [] $vis:vis struct $name:ident $($rest:tt)*) => {
        compile_error!(concat!("missing `#[proto(Command.SubCommand)]` on `", stringify!($name), "`"));
    };

    // base case: no fields left, emit the struct and its conversion
    (
        @munch [$(#[$meta:meta])* $vis:vis $name:ident $cmd:ident $sub_cmd:ident]
        [$($struct_field:tt)*]
        [$(($field:ident $type:ident [$($opt:tt)?] $key:ident))*]
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, ::bon::Builder)]
        $vis struct $name {
            $($struct_field)*
        }

        impl From<$name> for ::cta_protobuf::cta::admin::AdminCmd {
            #[allow(unused_variables, reason = "commands without options do not read `value`")]
            fn from(value: $name) -> Self {
                $crate::admin_cmd!($cmd.$sub_cmd {
                    $($key: $type $($opt)? => value.$field,)*
                })
            }
        }

        impl $crate::commands::TypedAdminCmd for $name {
            const CMD: ::cta_protobuf::cta::admin::admin_cmd::Cmd = ::cta_protobuf::cta::admin::admin_cmd::Cmd::$cmd;
            const SUBCMD: ::cta_protobuf::cta::admin::admin_cmd::SubCmd =
                ::cta_protobuf::cta::admin::admin_cmd::SubCmd::$sub_cmd;
        }
    };

    // one rule per option type: append the struct field and the `admin_cmd!` field
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : str ? => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* #[builder(into)] pub $field: Option<String>,]
            [$($c)* ($field str [?] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : str => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* #[builder(into)] pub $field: String,]
            [$($c)* ($field str [] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : u64 ? => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: Option<u64>,]
            [$($c)* ($field u64 [?] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : u64 => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: u64,]
            [$($c)* ($field u64 [] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : bool ? => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: Option<bool>,]
            [$($c)* ($field bool [?] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : bool => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: bool,]
            [$($c)* ($field bool [] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : flag => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* #[builder(default)] pub $field: bool,]
            [$($c)* ($field flag [] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : str_list ? => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: Option<Vec<String>>,]
            [$($c)* ($field str_list [?] $key)] $($($rest)*)?);
    };
    (@munch $head:tt [$($s:tt)*] [$($c:tt)*]
        $(#[$m:meta])* $field:ident : str_list => $key:ident $(, $($rest:tt)*)?) => {
        $crate::define_typed_admin_cmd!(@munch $head
            [$($s)* $(#[$m])* pub $field: Vec<String>,]
            [$($c)* ($field str_list [] $key)] $($($rest)*)?);
    };

    // Entry point
    ($($input:tt)*) => {
        $crate::define_typed_admin_cmd!(@attrs [] [] $($input)*);
    };
}
