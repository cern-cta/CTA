// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Admin commands: one module per command, one struct per subcommand.
//!
//! The sources are grouped by area in submodules, but every command module is exported directly here. Each struct
//! is defined with [`define_typed_admin_cmd!`](crate::define_typed_admin_cmd), has a `bon` builder and converts into
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

use cta_protobuf::cta::{
    admin::{
        AdminCmd,
        admin_cmd::{Cmd, SubCmd},
    },
    xrd::data::Data,
};

// One folder per domain (as in `cta-model` and `dao`), the command modules being re-exported here. Each command module
// is named after its `cta-admin` command, so the `admin` and `tape` folders are declared under other names, to leave
// `admin` and `tape` to the command modules.
#[path = "admin/mod.rs"]
mod admin_commands;
mod disk;
mod file;
mod library;
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

/// A typed admin command, implemented by [`define_typed_admin_cmd!`](crate::define_typed_admin_cmd).
pub trait TypedAdminCmd: Into<AdminCmd> {
    /// Protobuf command
    const CMD: Cmd;
    /// Protobuf subcommand ([`SubCmd::SubcmdNone`] for commands without one)
    const SUBCMD: SubCmd;
    /// Whether the frontend answers with a stream (`GenericAdminStream`) rather than a single response (`Admin`): every
    /// `ls`, and the commands without subcommand (`showqueues`, `version`). Streaming commands also implement
    /// [`StreamingAdminCmd`].
    const STREAMING: bool;
}

/// A typed admin command answered with a stream of records, implemented by
/// [`define_typed_admin_cmd!`](crate::define_typed_admin_cmd) for the commands with an `#[item(Variant: Type)]`.
pub trait StreamingAdminCmd: TypedAdminCmd {
    /// The protobuf record of each item of the stream (e.g. [`TapePoolLsItem`](cta_protobuf::cta::admin::TapePoolLsItem)
    /// for `tapepool ls`)
    type Item: std::fmt::Debug + serde::Serialize + Send + 'static;

    /// Extracts the record of a stream frame, giving the frame back if it carries a record of another kind.
    ///
    /// # Errors
    ///
    /// Returns `data` itself (boxed, as the enum is large) when it is not a [`Self::Item`].
    fn item(data: Data) -> Result<Self::Item, Box<Data>>;
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
