// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Admin commands: one module per command, one struct per subcommand.
//!
//! The sources are grouped by area in submodules, but every command module is exported directly here. Each struct
//! is defined with [`define_typed_admin_cmd!`](crate::define_typed_admin_cmd), has a `bon` builder and converts into
//! an [`AdminCmd`]:
//!
//! ```
//! use cta_client::commands::{TypedAdminCmd, tape_pool, virtual_organization};
//! use cta_protobuf::cta::admin::{AdminCmd, admin_cmd::SubCmd};
//!
//! let cmd: AdminCmd = tape_pool::Ls::builder().name("my_pool").build().into();
//!
//! let rm = virtual_organization::Rm::builder().vo("my_vo").build();
//! assert_eq!(virtual_organization::Rm::SUBCMD, SubCmd::SubcmdRm);
//! let cmd: AdminCmd = rm.into();
//! ```

use cta_protobuf::cta::admin::{
    AdminCmd,
    admin_cmd::{Cmd, SubCmd},
};

// One folder per area; the command modules are re-exported here
mod access;
mod disks;
mod files;
mod libraries;
mod mount_policies;
mod queues;
pub mod repack;
mod routing;
mod tapes;
pub mod version;

pub use access::{admin, virtual_organization};
pub use disks::{disk_instance, disk_instance_space, disk_system};
pub use files::{recycle_tape_file, tape_file};
pub use libraries::{drive, logical_library, media_type, physical_library};
pub use mount_policies::{
    activity_mount_rule, group_mount_rule, mount_policy, requester_mount_rule,
};
pub use queues::{failed_request, show_queues};
pub use routing::{archive_route, storage_class};
pub use tapes::{tape, tape_pool};

/// A typed admin command, implemented by [`define_typed_admin_cmd!`](crate::define_typed_admin_cmd).
pub trait TypedAdminCmd: Into<AdminCmd> {
    /// Protobuf command
    const CMD: Cmd;
    /// Protobuf subcommand ([`SubCmd::SubcmdNone`] for commands without one)
    const SUBCMD: SubCmd;
    /// Whether the frontend answers with a stream (`GenericAdminStream`) rather than a single response (`Admin`): every
    /// `ls`, and the commands without subcommand (`showqueues`, `version`)
    const STREAMING: bool = matches!(Self::SUBCMD, SubCmd::SubcmdLs | SubCmd::SubcmdNone);
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
