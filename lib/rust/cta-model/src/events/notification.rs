// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The encoding of the events as the protobuf notifications of the workflow API.

use cta_protobuf::cta::{
    common::{
        ChecksumBlob, OwnerId, RequesterId, Service,
        checksum_blob::{Checksum, checksum::Type as ProtoChecksumType},
    },
    eos::{Client, Metadata, Notification, Transport, Workflow, workflow::EventType},
};

use super::{Archive, CancelRetrieve, Create, Delete, FileInfo, InvalidEvent, Requester, Retrieve};

/// The `requester_instance` of the notifications built by this module.
pub const REQUESTER_INSTANCE: &str = "cta-send-event";

/// A trait sealed to this module, so that only the events of this crate can implement [`Event`].
mod sealed {
    pub trait Sealed {}
}

/// A workflow event ([`Create`], [`Archive`], [`Retrieve`], [`CancelRetrieve`] or [`Delete`]), with its encoding.
pub trait Event: sealed::Sealed {
    /// The type of the event, as in its notification (the EOS name of the event)
    const EVENT_TYPE: EventType;

    /// The notification of the event, sent by the disk instance `disk_instance`.
    ///
    /// # Errors
    ///
    /// The [`InvalidEvent`] of the `validate` of the event: the event is checked before being encoded.
    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent>;
}

/// The disk instance sending the event, with the URL of its service, if any
fn service(name: &str, url: Option<&str>) -> Service {
    Service {
        name: name.to_owned(),
        url: url.unwrap_or_default().to_owned(),
    }
}

/// The metadata of the file, without the fields specific to an event
fn metadata(file: &FileInfo) -> Metadata {
    Metadata {
        archive_file_id: file.archive_file_id.unwrap_or_default(),
        size: file.size.unwrap_or_default(),
        csb: (!file.checksums.is_empty()).then(|| ChecksumBlob {
            cs: file
                .checksums
                .iter()
                .map(|checksum| Checksum {
                    r#type: ProtoChecksumType::from(checksum.r#type).into(),
                    value: checksum.value.clone(),
                })
                .collect(),
        }),
        lpath: file.path.clone().unwrap_or_default(),
        xattr: file.xattr.clone().into_iter().collect(),
        disk_file_id: file.disk_file_id.clone().unwrap_or_default(),
        owner: file.owner.map(|owner| OwnerId {
            uid: owner.0,
            gid: owner.1,
        }),
        ..Metadata::default()
    }
}

/// The notification of an event of type `event`, without the fields specific to an event
fn notification(
    event: EventType,
    instance: Service,
    requester: &Requester,
    file: Metadata,
) -> Notification {
    Notification {
        wf: Some(Workflow {
            event: event.into(),
            instance: Some(instance),
            requester_instance: REQUESTER_INSTANCE.to_owned(),
            ..Workflow::default()
        }),
        cli: Some(Client {
            user: Some(RequesterId {
                username: requester.username.clone(),
                groupname: requester.groupname.clone(),
            }),
            sec: None,
        }),
        transport: Some(Transport::default()),
        file: Some(file),
        directory: None,
    }
}

impl sealed::Sealed for Create {}

impl Event for Create {
    const EVENT_TYPE: EventType = EventType::Create;

    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent> {
        self.validate()?;
        let file = Metadata {
            storage_class: self.storage_class.clone(),
            ..metadata(&self.file)
        };
        Ok(notification(
            Self::EVENT_TYPE,
            service(disk_instance, None),
            &self.requester,
            file,
        ))
    }
}

impl sealed::Sealed for Archive {}

impl Event for Archive {
    const EVENT_TYPE: EventType = EventType::Closew;

    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent> {
        self.validate()?;
        let file = Metadata {
            storage_class: self.storage_class.clone(),
            ..metadata(&self.file)
        };
        Ok(Notification {
            transport: Some(Transport {
                report_url: self.report_url.clone().unwrap_or_default(),
                error_report_url: self.error_report_url.clone().unwrap_or_default(),
                ..Transport::default()
            }),
            ..notification(
                Self::EVENT_TYPE,
                service(disk_instance, self.service_url.as_deref()),
                &self.requester,
                file,
            )
        })
    }
}

impl sealed::Sealed for Retrieve {}

impl Event for Retrieve {
    const EVENT_TYPE: EventType = EventType::Prepare;

    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent> {
        self.validate()?;
        let mut notification = notification(
            Self::EVENT_TYPE,
            service(disk_instance, self.service_url.as_deref()),
            &self.requester,
            metadata(&self.file),
        );
        if let Some(wf) = &mut notification.wf {
            wf.verify_only = self.verify_only;
            wf.vid = self.vid.clone().unwrap_or_default();
        }
        notification.transport = Some(Transport {
            dst_url: self.dst_url.clone(),
            error_report_url: self.error_report_url.clone().unwrap_or_default(),
            ..Transport::default()
        });
        Ok(notification)
    }
}

impl sealed::Sealed for CancelRetrieve {}

impl Event for CancelRetrieve {
    const EVENT_TYPE: EventType = EventType::AbortPrepare;

    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent> {
        self.validate()?;
        let file = Metadata {
            request_objectstore_id: self.request_objectstore_id.clone(),
            ..metadata(&self.file)
        };
        Ok(notification(
            Self::EVENT_TYPE,
            service(disk_instance, None),
            &self.requester,
            file,
        ))
    }
}

impl sealed::Sealed for Delete {}

impl Event for Delete {
    const EVENT_TYPE: EventType = EventType::Delete;

    fn notification(&self, disk_instance: &str) -> Result<Notification, InvalidEvent> {
        self.validate()?;
        Ok(notification(
            Self::EVENT_TYPE,
            service(disk_instance, None),
            &self.requester,
            metadata(&self.file),
        ))
    }
}
