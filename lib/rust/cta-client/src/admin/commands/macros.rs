// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! [`admin_cmd!`](crate::admin_cmd), building a one-off [`AdminCmd`] message. The typed commands are defined with the
//! [`#[typed_admin_cmd]`](crate::typed_admin_cmd) attribute instead, of the `cta-client-macros` crate.
//!
//! How a value is sent (which option list, whether it is sent at all) is decided by its type, through
//! [`OptionValue`](crate::admin::commands::OptionValue).
//!
//! [`AdminCmd`]: cta_protobuf::cta::admin::AdminCmd

/// Build an [`AdminCmd`] message.
///
/// ```text
/// admin_cmd!(Command.SubCommand {
///     Key: Type => expr,   // `Type::from(expr)`, added as the option `Key` of `Type`
/// })
/// ```
///
/// `Type` is any [`OptionValue`]: `String`/`&str`, `u64`, `bool`, `Vec<String>`, [`Flag`], the enums of `cta_model`,
/// or an `Option` of one of these (left out when `None`). `Key` is looked up among the keys of `Type`, so a key of the
/// wrong option list does not compile.
///
/// ```
/// # use cta_client::admin_cmd;
/// let cmd = admin_cmd!(Tapefile.SubcmdLs {
///     ArchiveFileId: u64 => 4294967296u64,
///     Vid: Option<&str> => None,
/// });
/// ```
///
/// [`AdminCmd`]: cta_protobuf::cta::admin::AdminCmd
/// [`OptionValue`]: crate::admin::commands::OptionValue
/// [`Flag`]: crate::admin::commands::Flag
#[macro_export]
macro_rules! admin_cmd {
    ($cmd:ident . $sub_cmd:ident { $($key:ident : $type:ty => $val:expr),* $(,)? }) => {{
        #[allow(unused_mut, reason = "commands without options are not modified")]
        let mut cmd = $crate::admin::commands::new_admin_cmd(
            $crate::__private::cta_protobuf::cta::admin::admin_cmd::Cmd::$cmd,
            $crate::__private::cta_protobuf::cta::admin::admin_cmd::SubCmd::$sub_cmd,
        );
        $(
            $crate::admin::commands::OptionValue::push(
                <$type>::from($val),
                &mut cmd,
                <$type as $crate::admin::commands::OptionValue>::Key::$key,
            );
        )*
        cmd
    }};
}
