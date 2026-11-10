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

use cta_protobuf::cta::{
    admin::{
        AdminCmd,
        admin_cmd::{Cmd, SubCmd},
    },
    xrd::data::Data,
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
