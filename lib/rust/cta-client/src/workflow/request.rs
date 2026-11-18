// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The requests of the workflow events of [`cta_model::events`]: the RPC each one is sent to, with its notification.

use cta_model::events::{Archive, CancelRetrieve, Create, Delete, Event, InvalidEvent, Retrieve};
use cta_protobuf::cta::{
    eos::Notification,
    xrd::{Request, request::Request as RequestType},
};

/// The RPC of the workflow API an event is sent to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::IntoStaticStr)]
pub enum WorkflowRpc {
    /// Reserve a file ID for a new file. Equivalent to `CREATE` from EOS
    Create,
    /// Archive a file. Equivalent to `CLOSEW` from EOS
    Archive,
    /// Retrieve a file. Equivalent to `PREPARE` from EOS
    Retrieve,
    /// Cancel a retrieval. Equivalent to `ABORT_PREPARE` from EOS
    CancelRetrieve,
    /// Delete a file. Equivalent to `DELETE` from EOS
    Delete,
}

/// A trait sealed to this module, so that only the events of [`cta_model::events`] can implement it.
mod sealed {
    pub trait Sealed {}
}

/// An event of the workflow API ([`Create`], [`Archive`], [`Retrieve`], [`CancelRetrieve`] or [`Delete`]), with the
/// RPC it is sent to. Its notification is built by [`cta_model::events::Event`].
pub trait WorkflowEvent: Event + sealed::Sealed {
    /// The RPC the event is sent to
    const RPC: WorkflowRpc;
}

macro_rules! workflow_events {
    ($($event:ident),*) => {$(
        impl sealed::Sealed for $event {}

        impl WorkflowEvent for $event {
            const RPC: WorkflowRpc = WorkflowRpc::$event;
        }
    )*};
}

workflow_events!(Create, Archive, Retrieve, CancelRetrieve, Delete);

/// The request of an event, ready to be sent with [`super::WorkflowClient::submit`].
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowRequest {
    rpc: WorkflowRpc,
    notification: Notification,
}

impl WorkflowRequest {
    /// The request sent for `event` by the disk instance `disk_instance`.
    ///
    /// # Errors
    ///
    /// See [`Event::notification`].
    pub fn new<E: WorkflowEvent>(event: &E, disk_instance: &str) -> Result<Self, InvalidEvent> {
        Ok(Self {
            rpc: E::RPC,
            notification: event.notification(disk_instance)?,
        })
    }

    /// The RPC the request is sent to.
    #[must_use]
    pub fn rpc(&self) -> WorkflowRpc {
        self.rpc
    }

    /// The notification of the event.
    #[must_use]
    pub fn notification(&self) -> &Notification {
        &self.notification
    }

    /// The request sent to the workflow API.
    #[must_use]
    pub fn request(&self) -> Request {
        Request {
            request: Some(RequestType::Notification(Box::new(
                self.notification.clone(),
            ))),
        }
    }

    /// The request sent to the workflow API, as proto3 JSON (lowerCamelCase keys, 64-bit integers as strings, enums
    /// as names). Requires the `json` feature.
    ///
    /// # Errors
    ///
    /// Returns the serialization error, which cannot happen for the messages built by this module.
    #[cfg(feature = "json")]
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self.request())
    }
}
