// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Administrators (`admin`)

use cta_client_macros::admin_dao;
use cta_model::admin::AdminUser;

use crate::admin::{AdminError, AdminStream, commands::admin, dao::find_one};

/// Administrators (`admin`): see [`AdminUser`].
#[admin_dao]
pub struct AdminUsers;

impl AdminUsers {
    /// The administrator named `name`.
    pub async fn get(&self, name: &str) -> Result<AdminUser, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "admin user",
            name,
            move |user: &AdminUser| user.name == wanted,
        )
        .await
    }

    /// All the administrators.
    pub async fn list(&self) -> Result<AdminStream<AdminUser>, AdminError> {
        self.client.stream_as(admin::Ls::default()).await
    }

    /// Creates an administrator.
    pub async fn create(&self, name: &str, comment: &str) -> Result<(), AdminError> {
        let cmd = admin::Add::builder().name(name).comment(comment).build();
        self.client.run(cmd).await
    }

    /// Changes the comment of an administrator.
    pub async fn update(&self, name: &str, comment: &str) -> Result<(), AdminError> {
        let cmd = admin::Ch::builder().name(name).comment(comment).build();
        self.client.run(cmd).await
    }

    /// Deletes an administrator.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(admin::Rm::builder().name(name).build())
            .await
    }
}
