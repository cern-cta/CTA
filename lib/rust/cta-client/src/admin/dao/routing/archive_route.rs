// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Archive routes

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::routing::{ArchiveRoute, ArchiveRouteType};

use crate::admin::{AdminError, AdminStream, commands::archive_route, dao::find_one};

/// Archive routes: see [`ArchiveRoute`].
#[admin_dao]
pub struct ArchiveRoutes;

#[bon]
impl ArchiveRoutes {
    /// The archive route of a copy of a storage class.
    pub async fn get(
        &self,
        storage_class_name: &str,
        copy_number: u32,
        r#type: ArchiveRouteType,
    ) -> Result<ArchiveRoute, AdminError> {
        let wanted = storage_class_name.to_owned();
        find_one(
            self.list().await?,
            "archive route",
            format!("{storage_class_name}/{copy_number}/{}", r#type),
            move |route: &ArchiveRoute| {
                route.storage_class_name == wanted
                    && route.copy_number == copy_number
                    && route.r#type == r#type
            },
        )
        .await
    }

    /// All the archive routes.
    pub async fn list(&self) -> Result<AdminStream<ArchiveRoute>, AdminError> {
        self.client.stream_as(archive_route::Ls::default()).await
    }

    /// Creates an archive route.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] storage_class_name: String,
        #[builder(start_fn)] copy_number: u32,
        #[builder(start_fn)] r#type: ArchiveRouteType,
        /// Tape pool the copy is written to
        #[builder(into)]
        tape_pool_name: String,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = archive_route::Add::builder()
            .storage_class(storage_class_name)
            .copy_nb(u64::from(copy_number))
            .archive_route_type(r#type)
            .tape_pool(tape_pool_name)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes an archive route; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] storage_class_name: String,
        #[builder(start_fn)] copy_number: u32,
        #[builder(start_fn)] r#type: ArchiveRouteType,
        /// New tape pool
        #[builder(into)]
        tape_pool_name: Option<String>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = archive_route::Ch::builder()
            .storage_class(storage_class_name)
            .copy_nb(u64::from(copy_number))
            .archive_route_type(r#type)
            .maybe_tape_pool(tape_pool_name)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes an archive route.
    pub async fn delete(
        &self,
        storage_class_name: &str,
        copy_number: u32,
        r#type: ArchiveRouteType,
    ) -> Result<(), AdminError> {
        let cmd = archive_route::Rm::builder()
            .storage_class(storage_class_name)
            .copy_nb(u64::from(copy_number))
            .archive_route_type(r#type)
            .build();
        self.client.run(cmd).await
    }
}
