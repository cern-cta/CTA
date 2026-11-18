// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Media types (`mediatype`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::tape::MediaType;

use crate::admin::{AdminError, AdminStream, commands::media_type, dao::find_one};

/// Media types (`mediatype`): see [`MediaType`].
#[admin_dao]
pub struct MediaTypes;

#[bon]
impl MediaTypes {
    /// The media type named `name`.
    pub async fn get(&self, name: &str) -> Result<MediaType, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().await?,
            "media type",
            name,
            move |media_type: &MediaType| media_type.name == wanted,
        )
        .await
    }

    /// All the media types.
    pub async fn list(&self) -> Result<AdminStream<MediaType>, AdminError> {
        self.client.stream_as(media_type::Ls::default()).await
    }

    /// Creates a media type.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Cartridge type
        #[builder(into)]
        cartridge_type: String,
        /// Capacity, in bytes
        capacity: u64,
        /// Primary density code
        #[builder(default)]
        primary_density_code: u64,
        /// Secondary density code
        #[builder(default)]
        secondary_density_code: u64,
        /// Comment
        #[builder(into)]
        comment: String,
    ) -> Result<(), AdminError> {
        let cmd = media_type::Add::builder()
            .name(name)
            .cartridge(cartridge_type)
            .capacity(capacity)
            .primary_density_code(primary_density_code)
            .secondary_density_code(secondary_density_code)
            .comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a media type; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New cartridge type
        #[builder(into)]
        cartridge_type: Option<String>,
        /// New primary density code
        primary_density_code: Option<u64>,
        /// New secondary density code
        secondary_density_code: Option<u64>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = media_type::Ch::builder()
            .name(name)
            .maybe_cartridge(cartridge_type)
            .maybe_primary_density_code(primary_density_code)
            .maybe_secondary_density_code(secondary_density_code)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a media type.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(media_type::Rm::builder().name(name).build())
            .await
    }
}
