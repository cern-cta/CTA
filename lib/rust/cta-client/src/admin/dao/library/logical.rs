// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Logical libraries

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::library::{LibraryState, LogicalLibrary};

use crate::admin::{AdminError, AdminStream, commands::logical_library, dao::find_one};

/// Logical libraries: see [`LogicalLibrary`].
#[admin_dao]
pub struct LogicalLibraries;

#[bon]
impl LogicalLibraries {
    /// The logical library named `name`.
    pub async fn get(&self, name: &str) -> Result<LogicalLibrary, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().call().await?,
            "logical library",
            name,
            move |library: &LogicalLibrary| library.name == wanted,
        )
        .await
    }

    /// The logical libraries, optionally those in a state.
    #[builder]
    pub async fn list(
        &self,
        /// Only the enabled or the disabled libraries
        state: Option<LibraryState>,
    ) -> Result<AdminStream<LogicalLibrary>, AdminError> {
        let cmd = logical_library::Ls::builder()
            .maybe_disabled(state.map(LibraryState::is_disabled))
            .build();
        self.client.stream_as(cmd).await
    }

    /// Creates a logical library.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Physical library hosting it
        #[builder(into)]
        physical_library_name: String,
        /// Initial state
        #[builder(default = LibraryState::Disabled)]
        state: LibraryState,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = logical_library::Add::builder()
            .name(name)
            .physical_library(physical_library_name)
            .disabled(state.is_disabled())
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a logical library; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New physical library
        #[builder(into)]
        physical_library_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = logical_library::Ch::builder()
            .name(name)
            .maybe_physical_library(physical_library_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Disables a logical library, giving a reason.
    pub async fn disable(&self, name: &str, reason: &str) -> Result<(), AdminError> {
        let cmd = logical_library::Ch::builder()
            .name(name)
            .disabled(true)
            .disabled_reason(reason)
            .build();
        self.client.run(cmd).await
    }

    /// Enables a logical library.
    pub async fn enable(&self, name: &str) -> Result<(), AdminError> {
        let cmd = logical_library::Ch::builder()
            .name(name)
            .disabled(false)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a logical library.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(logical_library::Rm::builder().name(name).build())
            .await
    }
}
