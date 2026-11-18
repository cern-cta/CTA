// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The queues of the scheduler

use bon::bon;
use cta_client_macros::admin_dao;
use cta_model::scheduler::{MountType, UserRequest};

use crate::admin::{AdminError, AdminStream, commands::show_queues::ShowQueues, dao::filtered};

/// The queues of the scheduler : see [`UserRequest`] and [`cta_model::scheduler::summarize`].
#[admin_dao]
pub struct UserRequests;

#[bon]
impl UserRequests {
    /// The queues, optionally those of one direction.
    #[builder]
    pub async fn list(
        &self,
        /// Only the archive or the retrieve queues
        mount_type: Option<MountType>,
    ) -> Result<AdminStream<UserRequest>, AdminError> {
        let queues = self.client.stream_as(ShowQueues::default()).await?;
        Ok(match mount_type {
            // `showqueues` has no filter: the direction is checked on the client side
            Some(mount_type) => filtered(queues, move |queue: &UserRequest| {
                queue.mount_type == mount_type
            }),
            None => queues,
        })
    }
}
