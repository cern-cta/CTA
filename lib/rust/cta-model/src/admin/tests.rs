// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of admin users, virtual organisations and versions, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use cta_protobuf::cta::admin::{AdminLsItem, Version, VersionItem, VirtualOrganizationLsItem};

use super::*;
use crate::{
    ConversionError,
    test_support::cta_admin_jsonl,
    tests::{convert, log, metadata},
    *,
};

mod admin_user {
    use super::*;

    #[test]
    fn converts_the_records() {
        let users = convert::<AdminLsItem, AdminUser>(cta_admin_jsonl!("admin_users.jsonl"));

        assert_eq!(users.len(), 3);
        assert_eq!(
            users[0],
            AdminUser {
                name: "afonso".into(),
                metadata: metadata(
                    log("rbachman", "cta-frontend01", 1_656_922_462),
                    log("rbachman", "cta-frontend01", 1_656_922_462),
                    "Joao Afonso",
                ),
            }
        );
        assert_eq!(users[1].key(), "ccaffy");
    }
}

mod virtual_organisation {
    use super::*;

    #[test]
    fn converts_the_records() {
        let vos = convert::<VirtualOrganizationLsItem, VirtualOrganisation>(cta_admin_jsonl!(
            "virtual_organisations.jsonl"
        ));

        assert_eq!(vos.len(), 3);
        assert_eq!(
            vos[0],
            VirtualOrganisation {
                name: "ATLAS".into(),
                disk_instance_name: "eosctaatlaspps".into(),
                is_repack_vo: false,
                max_read_drives: 2,
                max_write_drives: 2,
                max_file_size: 128_849_018_880,
                metadata: metadata(
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_175),
                    log("idlarbi", "ctapreproductionfrontend11", 1_739_458_822),
                    "ATLAS",
                ),
            }
        );
    }
}

mod version {
    use super::*;

    #[test]
    fn converts_the_records() {
        let item = VersionItem {
            client_version: Some(Version {
                cta_version: "5.11.12.1-1".into(),
                xrootd_ssi_protobuf_interface_version: "v1.2.3".into(),
            }),
            server_version: Some(Version {
                cta_version: "5.11.13.0-1".into(),
                xrootd_ssi_protobuf_interface_version: String::new(),
            }),
            catalogue_connection_string: "postgresql:cta@db/cta".into(),
            catalogue_version: "15.0".into(),
            is_upgrading: false,
            scheduler_backend_name: "cephUser".into(),
            instance_name: "preproduction".into(),
        };

        assert_eq!(
            CtaVersion::try_from(item.clone()).unwrap(),
            CtaVersion {
                client_version: ComponentVersion {
                    version: "5.11.12.1-1".into(),
                    protobuf_interface_version: Some("v1.2.3".into()),
                },
                server_version: ComponentVersion {
                    version: "5.11.13.0-1".into(),
                    protobuf_interface_version: None,
                },
                catalogue_connection_string: "postgresql:cta@db/cta".into(),
                catalogue_version: "15.0".into(),
                is_upgrading: false,
                scheduler_backend_name: "cephUser".into(),
                instance_name: "preproduction".into(),
            }
        );
        let incomplete = VersionItem {
            server_version: None,
            ..item
        };
        assert!(matches!(
            CtaVersion::try_from(incomplete).unwrap_err(),
            ConversionError::MissingField {
                record: "VersionItem",
                field: "server_version"
            }
        ));
    }
}
