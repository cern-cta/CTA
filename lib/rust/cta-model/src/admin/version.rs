// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Versions of the client and of the frontend

use cta_protobuf::cta::admin::{Version, VersionItem};
use serde::Serialize;

use crate::{
    ConversionError,
    convert::{non_empty, required},
};

/// The version of one side of the connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComponentVersion {
    /// CTA version
    pub version: String,
    /// Version of the protobuf interface (`xrootd-ssi-protobuf-interface`), if reported
    pub protobuf_interface_version: Option<String>,
}

impl From<Version> for ComponentVersion {
    fn from(version: Version) -> Self {
        Self {
            version: version.cta_version,
            protobuf_interface_version: non_empty(version.xrootd_ssi_protobuf_interface_version),
        }
    }
}

/// The versions of the client and of the frontend, and the deployment the frontend serves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CtaVersion {
    /// This client, as echoed by the frontend
    pub client_version: ComponentVersion,
    /// The frontend
    pub server_version: ComponentVersion,
    /// Connection string of the catalogue of the frontend
    pub catalogue_connection_string: String,
    /// Schema version of the catalogue
    pub catalogue_version: String,
    /// Whether the catalogue schema is being upgraded
    pub is_upgrading: bool,
    /// Scheduler backend of the frontend
    pub scheduler_backend_name: String,
    /// Name of the CTA instance
    pub instance_name: String,
}

impl TryFrom<VersionItem> for CtaVersion {
    type Error = ConversionError;

    fn try_from(item: VersionItem) -> Result<Self, Self::Error> {
        const RECORD: &str = "VersionItem";
        Ok(Self {
            client_version: required(RECORD, "client_version", item.client_version)?.into(),
            server_version: required(RECORD, "server_version", item.server_version)?.into(),
            catalogue_connection_string: item.catalogue_connection_string,
            catalogue_version: item.catalogue_version,
            is_upgrading: item.is_upgrading,
            scheduler_backend_name: item.scheduler_backend_name,
            instance_name: item.instance_name,
        })
    }
}
