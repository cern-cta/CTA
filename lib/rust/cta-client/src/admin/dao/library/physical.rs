// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Physical libraries (`physicallibrary`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::library::{LibraryState, PhysicalLibrary};

use crate::admin::{
    AdminError, AdminStream,
    commands::physical_library,
    dao::{filtered, find_one},
};

/// Physical libraries (`physicallibrary`): see [`PhysicalLibrary`].
#[admin_dao]
pub struct PhysicalLibraries;

#[bon]
impl PhysicalLibraries {
    /// The physical library named `name`.
    pub async fn get(&self, name: &str) -> Result<PhysicalLibrary, AdminError> {
        let wanted = name.to_owned();
        find_one(
            self.list().call().await?,
            "physical library",
            name,
            move |library: &PhysicalLibrary| library.name == wanted,
        )
        .await
    }

    /// The physical libraries, optionally those in a state.
    #[builder]
    pub async fn list(
        &self,
        /// Only the enabled or the disabled libraries
        state: Option<LibraryState>,
    ) -> Result<AdminStream<PhysicalLibrary>, AdminError> {
        let libraries = self
            .client
            .stream_as(physical_library::Ls::default())
            .await?;
        Ok(match state {
            // `physicallibrary ls` has no filter: the state is checked on the client side
            Some(state) => filtered(libraries, move |library: &PhysicalLibrary| {
                library.state == state
            }),
            None => libraries,
        })
    }

    /// Creates a physical library.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        /// Manufacturer
        #[builder(into)]
        manufacturer: String,
        /// Model
        #[builder(into)]
        model: String,
        /// Number of cartridge slots
        physical_cartridge_slots: u64,
        /// Number of drive slots
        physical_drive_slots: u64,
        /// URL of the management interface
        #[builder(into)]
        gui_url: Option<String>,
        /// URL of the webcam
        #[builder(into)]
        webcam_url: Option<String>,
        /// Cartridge technology
        #[builder(into)]
        r#type: Option<String>,
        /// Where the library is
        #[builder(into)]
        location: Option<String>,
        /// Number of free cartridge slots
        available_cartridge_slots: Option<u64>,
        /// Comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = physical_library::Add::builder()
            .name(name)
            .manufacturer(manufacturer)
            .model(model)
            .physical_cartridge_slots(physical_cartridge_slots)
            .physical_drive_slots(physical_drive_slots)
            .maybe_gui_url(gui_url)
            .maybe_webcam_url(webcam_url)
            .maybe_library_type(r#type)
            .maybe_location(location)
            .maybe_available_cartridge_slots(available_cartridge_slots)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Changes a physical library; the arguments left out are unchanged.
    #[builder]
    pub async fn update(
        &self,
        #[builder(start_fn, into)] name: String,
        /// New URL of the management interface
        #[builder(into)]
        gui_url: Option<String>,
        /// New URL of the webcam
        #[builder(into)]
        webcam_url: Option<String>,
        /// New location
        #[builder(into)]
        location: Option<String>,
        /// New number of cartridge slots
        physical_cartridge_slots: Option<u64>,
        /// New number of free cartridge slots
        available_cartridge_slots: Option<u64>,
        /// New number of drive slots
        physical_drive_slots: Option<u64>,
        /// New comment
        #[builder(into)]
        comment: Option<String>,
    ) -> Result<(), AdminError> {
        let cmd = physical_library::Ch::builder()
            .name(name)
            .maybe_gui_url(gui_url)
            .maybe_webcam_url(webcam_url)
            .maybe_location(location)
            .maybe_physical_cartridge_slots(physical_cartridge_slots)
            .maybe_available_cartridge_slots(available_cartridge_slots)
            .maybe_physical_drive_slots(physical_drive_slots)
            .maybe_comment(comment)
            .build();
        self.client.run(cmd).await
    }

    /// Disables a physical library, giving a reason.
    pub async fn disable(&self, name: &str, reason: &str) -> Result<(), AdminError> {
        let cmd = physical_library::Ch::builder()
            .name(name)
            .disabled(true)
            .state_reason(reason)
            .build();
        self.client.run(cmd).await
    }

    /// Enables a physical library.
    pub async fn enable(&self, name: &str) -> Result<(), AdminError> {
        let cmd = physical_library::Ch::builder()
            .name(name)
            .disabled(false)
            .build();
        self.client.run(cmd).await
    }

    /// Deletes a physical library.
    pub async fn delete(&self, name: &str) -> Result<(), AdminError> {
        self.client
            .run(physical_library::Rm::builder().name(name).build())
            .await
    }
}
