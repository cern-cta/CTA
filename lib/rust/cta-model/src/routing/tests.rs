// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Conversion of the records of storage classes, archive routes, mount policies and mount rules, replaying the `cta-admin --jsonl` samples of the `cta-sdk` unit tests (see
//! [`crate::tests`]).

use std::time::Duration;

use cta_protobuf::cta::admin::{
    ActivityMountRuleLsItem, ArchiveRouteLsItem, GroupMountRuleLsItem, MountPolicyLsItem,
    RequesterMountRuleLsItem, StorageClassLsItem,
};

use super::*;
use crate::{
    test_support::cta_admin_jsonl,
    tests::{convert, log, metadata},
    *,
};

mod storage_class {
    use super::*;

    #[test]
    fn converts_the_records() {
        let classes =
            convert::<StorageClassLsItem, StorageClass>(cta_admin_jsonl!("storage_classes.jsonl"));

        assert_eq!(classes.len(), 3);
        assert_eq!(
            classes[0],
            StorageClass {
                name: "atlaspps".into(),
                virtual_organisation_name: "ATLAS".into(),
                replica_count: 1,
                metadata: metadata(
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_233),
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_233),
                    "atlaspps storage class on physical tapes",
                ),
            }
        );
        assert_eq!(classes[2].replica_count, 2);
    }
}

mod archive_route {
    use super::*;

    #[test]
    fn converts_the_records() {
        let routes =
            convert::<ArchiveRouteLsItem, ArchiveRoute>(cta_admin_jsonl!("archive_routes.jsonl"));

        assert_eq!(routes.len(), 3);
        assert_eq!(
            routes[0],
            ArchiveRoute {
                storage_class_name: "atlaspps".into(),
                copy_number: 1,
                route_type: ArchiveRouteType::Default,
                tape_pool_name: "atlaspps".into(),
                metadata: metadata(
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_381),
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_381),
                    "atlaspps storage class on physical tapes",
                ),
            }
        );
        assert_eq!(routes[0].key(), ("atlaspps", 1, ArchiveRouteType::Default));
        assert_eq!(<&str>::from(ArchiveRouteType::Repack), "REPACK");
    }
}

mod mount_policy {
    use super::*;

    #[test]
    fn converts_the_records() {
        let policies =
            convert::<MountPolicyLsItem, MountPolicy>(cta_admin_jsonl!("mount_policies.jsonl"));

        assert_eq!(policies.len(), 3);
        assert_eq!(
            policies[0],
            MountPolicy {
                name: "ctaatlaspps".into(),
                archive_priority: 20,
                min_archive_age: Duration::from_secs(1200),
                retrieve_priority: 10,
                min_retrieve_age: Duration::from_secs(1200),
                metadata: metadata(
                    log("jleduc", "eosctafst0017.cern.ch", 1_593_464_467),
                    log("jleduc", "cta-frontend01", 1_677_595_224),
                    "Mount policy for Atlas",
                ),
            }
        );
    }
}

mod mount_rule {
    use super::*;

    #[test]
    fn user() {
        let rules = convert::<RequesterMountRuleLsItem, UserMountRule>(cta_admin_jsonl!(
            "user_mount_rules.jsonl"
        ));

        assert_eq!(rules.len(), 3);
        assert_eq!(
            rules[0],
            UserMountRule {
                user_name: "ams".into(),
                disk_instance_name: "eosctaatlaspps".into(),
                mount_policy_name: "rucioit".into(),
                metadata: metadata(
                    log("jleduc", "ctapreproductionfrontend11", 1_746_433_481),
                    log("jleduc", "ctapreproductionfrontend11", 1_746_438_309),
                    "AMS for rucio IT Service service tests (operations #1644)",
                ),
            }
        );
        assert_eq!(rules[0].key(), ("ams", "eosctaatlaspps"));
    }

    #[test]
    fn group() {
        let rules = convert::<GroupMountRuleLsItem, GroupMountRule>(cta_admin_jsonl!(
            "group_mount_rules.jsonl"
        ));

        assert_eq!(rules.len(), 3);
        assert_eq!(
            rules[0],
            GroupMountRule {
                group_name: "ppstestgroup".into(),
                disk_instance_name: "eosctapublicpps".into(),
                mount_policy_name: "ctapublicpps".into(),
                metadata: metadata(
                    log("vyurchen", "eosctafst0108.cern.ch", 1_623_825_495),
                    log("vyurchen", "eosctafst0108.cern.ch", 1_623_825_495),
                    "Group for tests",
                ),
            }
        );
    }

    #[test]
    fn activity() {
        let rules = convert::<ActivityMountRuleLsItem, ActivityMountRule>(cta_admin_jsonl!(
            "activity_mount_rules.jsonl"
        ));

        assert_eq!(rules.len(), 3);
        assert_eq!(
            rules[0],
            ActivityMountRule {
                user_name: "ctaops".into(),
                disk_instance_name: "eosctaatlaspps".into(),
                regex: "^Julien is Staging$".into(),
                mount_policy_name: "express".into(),
                metadata: metadata(
                    log("jleduc", "ctapreproductionfrontend11", 1_744_112_203),
                    log("jleduc", "ctapreproductionfrontend11", 1_744_112_203),
                    "preproduction activity with spaces and case mix",
                ),
            }
        );
        assert_eq!(
            rules[0].key(),
            ("ctaops", "eosctaatlaspps", "^Julien is Staging$")
        );
    }
}
