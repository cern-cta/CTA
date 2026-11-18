// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The versions of the client and of the frontend

use cta_model::admin::CtaVersion;

use crate::admin::{AdminClient, AdminError, commands::version::Version, dao::exactly_one};

impl AdminClient {
    /// The versions of this client and of the frontend (`version`).
    pub async fn version(&self) -> Result<CtaVersion, AdminError> {
        let stream = self.stream_as(Version::default()).await?;
        exactly_one(stream, "version", "").await
    }
}
