// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Admin commands: one module per command, one struct per subcommand.
//!
//! The sources are grouped by domain in submodules (as the types of `cta-model` and the data access objects), but
//! every command module is exported directly here. Each struct
//! is defined with [`#[typed_admin_cmd]`](crate::typed_admin_cmd), has a `bon` builder and converts into
//! an [`AdminCmd`]:
//!
//! ```
//! use cta_client::admin::commands::{TypedAdminCmd, tape_pool, virtual_organization};
//! use cta_protobuf::cta::admin::{AdminCmd, admin_cmd::SubCmd};
//!
//! let cmd: AdminCmd = tape_pool::Ls::builder().name("my_pool").build().into();
//!
//! let rm = virtual_organization::Rm::builder().vo("my_vo").build();
//! assert_eq!(virtual_organization::Rm::SUBCMD, SubCmd::SubcmdRm);
//! let cmd: AdminCmd = rm.into();
//! ```

use cta_model::{routing::ArchiveRouteType, tape::TapeState};
use cta_protobuf::{
    TryIntoError,
    cta::{
        admin::{
            AdminCmd, OptionBoolean, OptionStrList, OptionString, OptionUInt64,
            admin_cmd::{Cmd, SubCmd},
            option_boolean, option_str_list, option_string, option_u_int64,
        },
        xrd::data::Data,
    },
};

// One folder per domain (as in `cta-model` and `dao`), the command modules being re-exported here. Each command module
// is named after its `cta-admin` command, so the `admin` and `tape` folders are declared under other names, to leave
// `admin` and `tape` to the command modules.
#[path = "admin/mod.rs"]
mod admin_commands;
mod disk;
mod file;
mod library;
mod macros;
mod routing;
mod scheduler;
#[path = "tape/mod.rs"]
mod tape_commands;

pub use admin_commands::{admin, version, virtual_organization};
pub use disk::{disk_instance, disk_instance_space, disk_system};
pub use file::{recycle_tape_file, tape_file};
pub use library::{drive, logical_library, physical_library};
pub use routing::{
    activity_mount_rule, archive_route, group_mount_rule, mount_policy, requester_mount_rule,
    storage_class,
};
pub use scheduler::{failed_request, repack, show_queues};
pub use tape_commands::{media_type, tape, tape_pool};

/// A typed admin command, implemented by [`#[typed_admin_cmd]`](crate::typed_admin_cmd).
///
/// Every typed command is either a [`UnaryAdminCmd`] or a [`StreamingAdminCmd`], which tells the frontend service it
/// goes to.
pub trait TypedAdminCmd: Into<AdminCmd> {
    /// Protobuf command
    const CMD: Cmd;
    /// Protobuf subcommand ([`SubCmd::SubcmdNone`] for commands without one)
    const SUBCMD: SubCmd;
}

/// A typed admin command answered with a single response, by `CtaRpc.Admin`: every command but the streaming ones.
/// Implemented by [`#[typed_admin_cmd]`](crate::typed_admin_cmd) for the commands without `response = ...`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a unary admin command",
    note = "streaming commands (every `ls`, `showqueues`, `version`) are sent with `AdminClient::stream`"
)]
pub trait UnaryAdminCmd: TypedAdminCmd {}

/// A typed admin command answered with a stream of records, by `CtaRpcStream.GenericAdminStream`: every `ls`, and the
/// commands without subcommand (`showqueues`, `version`). Implemented by
/// [`#[typed_admin_cmd]`](crate::typed_admin_cmd) for the commands with a `response = Record`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a streaming admin command",
    note = "unary commands are sent with `AdminClient::unary`"
)]
pub trait StreamingAdminCmd: TypedAdminCmd {
    /// The protobuf record of each item of the stream (e.g. [`TapePoolLsItem`](cta_protobuf::cta::admin::TapePoolLsItem)
    /// for `tapepool ls`)
    ///
    /// Any record of [`Data`], each of them implementing `TryFrom<Data>` (derived by `cta-protobuf`).
    type Item: TryFrom<Data, Error = TryIntoError<Data>> + std::fmt::Debug + Send + 'static;

    /// Extracts the record of a stream frame, giving the frame back if it carries a record of another kind.
    ///
    /// # Errors
    ///
    /// Returns `data` itself (boxed, as the enum is large) when it is not a [`Self::Item`].
    fn item(data: Data) -> Result<Self::Item, Box<Data>> {
        Self::Item::try_from(data).map_err(|error| Box::new(error.input))
    }
}

/// A value of an option of an admin command: the type of its keys, and how it is added to the command.
///
/// Used by [`admin_cmd!`](crate::admin_cmd) and [`#[typed_admin_cmd]`](crate::typed_admin_cmd) for every
/// option, the key of an option being resolved among the `Key` of its type. Implement it for a type to use it as an
/// option.
pub trait OptionValue {
    /// The keys of the options of this type (e.g. `option_string::Key`)
    type Key: Into<i32>;

    /// Adds the option `key`, with this value, to `cmd`.
    fn push(self, cmd: &mut AdminCmd, key: Self::Key);
}

/// [`OptionString`]
impl OptionValue for String {
    type Key = option_string::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        cmd.option_str.push(OptionString {
            key: key.into(),
            value: self,
        });
    }
}

/// [`OptionString`]
impl OptionValue for &str {
    type Key = option_string::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        self.to_owned().push(cmd, key);
    }
}

/// [`OptionUInt64`]
impl OptionValue for u64 {
    type Key = option_u_int64::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        cmd.option_uint64.push(OptionUInt64 {
            key: key.into(),
            value: self,
        });
    }
}

/// [`OptionBoolean`]
impl OptionValue for bool {
    type Key = option_boolean::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        cmd.option_bool.push(OptionBoolean {
            key: key.into(),
            value: self,
        });
    }
}

/// [`OptionStrList`]
impl OptionValue for Vec<String> {
    type Key = option_str_list::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        cmd.option_str_list.push(OptionStrList {
            key: key.into(),
            item: self,
        });
    }
}

/// An optional option, left out when `None`
impl<T: OptionValue> OptionValue for Option<T> {
    type Key = T::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        if let Some(value) = self {
            value.push(cmd, key);
        }
    }
}

/// An [`OptionBoolean`] sent (as `true`) only when set, as the frontend tells an absent option from a `false`
/// one. Typically the wire type of a `bool` field (`#[proto(All, only_if_true)] all: bool`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flag(pub bool);

impl From<bool> for Flag {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl OptionValue for Flag {
    type Key = option_boolean::Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        if self.0 {
            true.push(cmd, key);
        }
    }
}

/// The enums of `cta_model` sent as an [`OptionString`], spelled as on the wire (`strum::IntoStaticStr`)
macro_rules! option_value_as_str {
    ($($type:ty),* $(,)?) => {
        $(
            impl OptionValue for $type {
                type Key = option_string::Key;

                fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
                    <&'static str>::from(self).push(cmd, key);
                }
            }
        )*
    };
}

option_value_as_str!(ArchiveRouteType, TapeState);

/// An [`AdminCmd`] without options: its command, subcommand and the versions of this client.
///
/// The starting point of [`admin_cmd!`](crate::admin_cmd) and [`#[typed_admin_cmd]`](crate::typed_admin_cmd), which
/// then add the options.
#[must_use]
pub fn new_admin_cmd(cmd: Cmd, sub_cmd: SubCmd) -> AdminCmd {
    AdminCmd {
        cmd: cmd.into(),
        subcmd: sub_cmd.into(),
        client_version: CLIENT_VERSION.to_string(),
        protobuf_tag: PROTOBUF_TAG.to_string(),
        ..Default::default()
    }
}

/// The client version sent in the `client_version` field of every admin command.
///
/// Generated at build time (see `build.rs`)
pub const CLIENT_VERSION: &str = env!("CTA_CLIENT_VERSION");

/// The protobuf interface version sent in the `protobuf_tag` field of every admin command.
///
/// Generated at build time  (see `build.rs`)
pub const PROTOBUF_TAG: &str = env!("CTA_CLIENT_PROTOBUF_TAG");

#[cfg(test)]
mod tests;
