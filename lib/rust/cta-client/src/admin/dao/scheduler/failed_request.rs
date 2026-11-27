// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Failed requests (`failedrequest`)

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::scheduler::{FailedRequest, FailedRequestSummary, MountType};
use cta_protobuf::cta::admin::RequestType;
use tokio_stream::StreamExt;

use crate::admin::{AdminError, AdminStream, commands::failed_request};

/// Failed requests: see [`FailedRequest`].
#[admin_dao]
pub struct FailedRequests;

#[bon]
impl FailedRequests {
    /// The failed requests matching the filters (all of them without filters).
    #[builder]
    pub async fn list(
        &self,
        /// Only the archive or the retrieve requests
        mount_type: Option<MountType>,
        /// Only the requests for this tape pool
        #[builder(into)]
        tape_pool_name: Option<String>,
        /// Only the requests for this tape
        #[builder(into)]
        vid: Option<String>,
        /// Include the error messages of the attempts
        #[builder(default)]
        logs: bool,
    ) -> Result<AdminStream<FailedRequest>, AdminError> {
        let cmd = failed_request::Ls::builder()
            .just_archive(mount_type == Some(MountType::Archive))
            .just_retrieve(mount_type == Some(MountType::Retrieve))
            .maybe_tape_pool_name(tape_pool_name)
            .maybe_vid(vid)
            .log(logs)
            .build();
        self.client.stream_as(cmd).await
    }

    /// Deletes a failed request.
    pub async fn delete(&self, id: &str) -> Result<(), AdminError> {
        self.client
            .run(failed_request::Rm::builder().id(id).build())
            .await
    }

    /// The failed requests counted per direction (without the overall total).
    pub async fn summary(&self) -> Result<Vec<FailedRequestSummary>, AdminError> {
        self.client
            .stream(failed_request::Summary::default())
            .await?
            .filter(
                |item| !matches!(item, Ok(summary) if summary.request_type() == RequestType::Total),
            )
            .map(|item| FailedRequestSummary::try_from(item?).map_err(AdminError::from))
            .collect()
            .await
    }
}
