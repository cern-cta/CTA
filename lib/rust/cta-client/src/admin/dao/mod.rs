// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Data access objects: typed queries on the resources of the catalogue and of the scheduler.
//!
//! One object per resource (a unit struct marked `#[admin_dao]`, see `cta_client_macros::admin_dao`), obtained from an
//! [`AdminClient`](super::AdminClient) (`client.tape_pools()`, ...). Each has the operations of the resource (`get`,
//! `list`, `create`, `update`, `delete`, and the specific ones: `drives().down()`, `tapes().reclaim()`, ...), built on
//! the typed commands of [`super::commands`] and returning the domain types of [`cta_model`].
//!
//! ```no_run
//! use std::time::Duration;
//! use cta_client::admin::AdminClient;
//! use tokio_stream::StreamExt;
//!
//! # async fn example(client: AdminClient) -> Result<(), Box<dyn std::error::Error>> {
//! let pool = client.tape_pools().get("my_pool").await?;
//!
//! client
//!     .mount_policies()
//!     .create("my_policy")
//!     .archive_priority(10)
//!     .min_archive_age(Duration::from_secs(600))
//!     .comment("for the tests")
//!     .call()
//!     .await?;
//!
//! let mut tapes = client.tapes().list().tape_pool_name(&pool.name).call().await?;
//! while let Some(tape) = tapes.next().await {
//!     println!("{}", tape?.vid);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Errors
//!
//! Every method returns the [`AdminError`] of the command it sends (user errors, API failures, transport failures,
//! records that cannot be converted). The `get` methods also return [`AdminError::NotFound`] when no record matches
//! and [`AdminError::MoreThanOne`] when several do, and the methods taking a file of identifiers return
//! [`AdminError::IdFile`] when it cannot be read.

#![allow(
    clippy::missing_errors_doc,
    reason = "every method can fail with AdminError (documented in the module for succinctness)"
)]

use std::fmt::Display;

use tokio_stream::StreamExt;

use crate::admin::{AdminError, AdminStream};

mod admin;
mod disk;
mod file;
mod id_file;
mod library;
mod routing;
mod scheduler;
mod tape;

pub use admin::*;
pub use disk::*;
pub use file::*;
pub use id_file::{IdFileError, read_disk_file_ids};
pub use library::*;
pub use routing::*;
pub use scheduler::*;
pub use tape::*;

/// The only item of `stream`: [`AdminError::NotFound`] if there is none, [`AdminError::MoreThanOne`] if there are
/// several.
pub(crate) async fn exactly_one<T>(
    mut stream: AdminStream<T>,
    resource: &'static str,
    id: impl Display,
) -> Result<T, AdminError> {
    let Some(first) = stream.next().await.transpose()? else {
        return Err(AdminError::NotFound {
            resource,
            id: id.to_string(),
        });
    };
    let mut count = 1;
    while let Some(item) = stream.next().await {
        item?;
        count += 1;
    }
    if count > 1 {
        return Err(AdminError::MoreThanOne {
            resource,
            id: id.to_string(),
            count,
        });
    }
    Ok(first)
}

/// The items of `stream` matching `keep`.
pub(crate) fn filtered<T: Send + 'static>(
    stream: AdminStream<T>,
    mut keep: impl FnMut(&T) -> bool + Send + 'static,
) -> AdminStream<T> {
    Box::pin(stream.filter(move |item| match item {
        Ok(item) => keep(item),
        Err(_) => true,
    }))
}

/// The only item of `stream` matching `keep`, for the resources whose `ls` has no filter on the key (see
/// [`exactly_one`] for the errors).
pub(crate) async fn find_one<T: Send + 'static>(
    stream: AdminStream<T>,
    resource: &'static str,
    id: impl Display,
    keep: impl FnMut(&T) -> bool + Send + 'static,
) -> Result<T, AdminError> {
    exactly_one(filtered(stream, keep), resource, id).await
}

/// All the items of `stream`.
pub(crate) async fn collect<T>(stream: AdminStream<T>) -> Result<Vec<T>, AdminError> {
    stream.collect().await
}

/// All the items of `stream`, which must not be empty ([`AdminError::NotFound`] otherwise).
pub(crate) async fn at_least_one<T>(
    stream: AdminStream<T>,
    resource: &'static str,
    id: impl Display,
) -> Result<Vec<T>, AdminError> {
    let items = collect(stream).await?;
    if items.is_empty() {
        return Err(AdminError::NotFound {
            resource,
            id: id.to_string(),
        });
    }
    Ok(items)
}

#[cfg(test)]
mod tests;
