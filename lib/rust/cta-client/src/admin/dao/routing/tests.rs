// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of storage classes, archive routes, mount policies and mount rules against the mock frontend
//! (see [`crate::admin::dao::tests`]).

use std::time::Duration;

use cta_model::routing::ArchiveRouteType;
use cta_protobuf::cta::{
    admin::{
        ActivityMountRuleLsItem, ArchiveRouteLsItem, GroupMountRuleLsItem, MountPolicyLsItem,
        RequesterMountRuleLsItem, StorageClassLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};

use crate::admin::{
    AdminError,
    dao::tests::{Sent, failure, listed, pair, run, sent, str_options, u64_options},
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

mod mount_policy {
    use super::*;

    #[tokio::test]
    async fn create_with_defaults() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .mount_policies()
            .create("ctaops")
            .comment("Mount policy for CTA operations")
            .call()
            .await
            .unwrap();

        let call = single_call(&calls);
        assert_eq!(call.rpc, "Admin");
        assert_eq!(pair(&call.cmd), (Cmd::Mountpolicy, SubCmd::SubcmdAdd));
        assert_eq!(
            str_options(&call.cmd),
            [
                (StrKey::MountPolicy, "ctaops"),
                (StrKey::Comment, "Mount policy for CTA operations")
            ]
        );
        assert_eq!(
            u64_options(&call.cmd),
            [
                (U64Key::ArchivePriority, 250),
                (U64Key::MinArchiveRequestAge, 14_400),
                (U64Key::RetrievePriority, 150),
                (U64Key::MinRetrieveRequestAge, 14_400),
            ]
        );
    }

    #[tokio::test]
    async fn update_sends_only_the_changes() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .mount_policies()
            .update("ctaops")
            .min_retrieve_age(Duration::from_secs(90))
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Mountpolicy, SubCmd::SubcmdCh));
        assert_eq!(str_options(&cmd), [(StrKey::MountPolicy, "ctaops")]);
        assert_eq!(u64_options(&cmd), [(U64Key::MinRetrieveRequestAge, 90)]);
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<MountPolicyLsItem>(cta_admin_jsonl!("mount_policies.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::MplsItem));
        let (policy, cmd) = run(
            reply,
            |c| async move { c.mount_policies().get("ctaops").await },
        )
        .await;
        assert_eq!(policy.name, "ctaops");
        assert_eq!(cmd, Sent::new(Cmd::Mountpolicy, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.mount_policies().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Mountpolicy, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move { c.mount_policies().delete("ctaops").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Mountpolicy, SubCmd::SubcmdRm).str(StrKey::MountPolicy, "ctaops")
        );
    }
}

mod archive_route {
    use super::*;

    #[tokio::test]
    async fn key_is_sent_as_strings_and_numbers() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .archive_routes()
            .delete("sc", 2, ArchiveRouteType::Repack)
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Archiveroute, SubCmd::SubcmdRm));
        assert_eq!(
            str_options(&cmd),
            [
                (StrKey::StorageClass, "sc"),
                (StrKey::ArchiveRouteType, "REPACK")
            ]
        );
        assert_eq!(u64_options(&cmd), [(U64Key::CopyNumber, 2)]);
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<ArchiveRouteLsItem>(cta_admin_jsonl!("archive_routes.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::ArlsItem));
        let (route, cmd) = run(reply.clone(), |c| async move {
            c.archive_routes()
                .get("cbacktest", 1, ArchiveRouteType::Default)
                .await
        })
        .await;
        assert_eq!(route.storage_class_name, "cbacktest");
        assert_eq!(cmd, Sent::new(Cmd::Archiveroute, SubCmd::SubcmdLs));
        // the copy number and the type are part of the key
        for (copy_number, route_type) in [
            (2, ArchiveRouteType::Default),
            (1, ArchiveRouteType::Repack),
        ] {
            let error = failure(reply.clone(), move |c| async move {
                c.archive_routes()
                    .get("cbacktest", copy_number, route_type)
                    .await
            })
            .await;
            assert!(
                matches!(
                    error,
                    AdminError::NotFound {
                        resource: "archive route",
                        ..
                    }
                ),
                "{error}"
            );
        }

        let cmd = listed(|c| async move { c.archive_routes().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Archiveroute, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move {
            c.archive_routes()
                .create("sc", 2, ArchiveRouteType::Repack)
                .tape_pool_name("tp")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Archiveroute, SubCmd::SubcmdAdd)
                .str(StrKey::StorageClass, "sc")
                .str(StrKey::ArchiveRouteType, "REPACK")
                .str(StrKey::TapePool, "tp")
                .str(StrKey::Comment, "c")
                .u64(U64Key::CopyNumber, 2)
        );

        let cmd = sent(|c| async move {
            c.archive_routes()
                .update("sc", 1, ArchiveRouteType::Default)
                .tape_pool_name("tp2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Archiveroute, SubCmd::SubcmdCh)
                .str(StrKey::StorageClass, "sc")
                .str(StrKey::ArchiveRouteType, "DEFAULT")
                .str(StrKey::TapePool, "tp2")
                .u64(U64Key::CopyNumber, 1)
        );
    }
}

mod storage_class {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<StorageClassLsItem>(cta_admin_jsonl!("storage_classes.jsonl"));
        let reply = Reply::records([Data::SclsItem(records[1].clone())]);
        let (class, cmd) = run(reply, |c| async move {
            c.storage_classes().get("cbacktest").await
        })
        .await;
        assert_eq!(class.name, "cbacktest");
        assert_eq!(
            cmd,
            Sent::new(Cmd::Storageclass, SubCmd::SubcmdLs).str(StrKey::StorageClass, "cbacktest")
        );

        let cmd =
            listed(|c| async move { c.storage_classes().list().vid("V01007").call().await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Storageclass, SubCmd::SubcmdLs).str(StrKey::Vid, "V01007")
        );

        let cmd = sent(|c| async move {
            c.storage_classes()
                .create("sc")
                .virtual_organisation_name("ATLAS")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Storageclass, SubCmd::SubcmdAdd)
                .str(StrKey::StorageClass, "sc")
                .str(StrKey::Vo, "ATLAS")
                .str(StrKey::Comment, "c")
                .u64(U64Key::CopyNumber, 1)
        );

        let cmd = sent(|c| async move {
            c.storage_classes()
                .update("sc")
                .replica_count(2)
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Storageclass, SubCmd::SubcmdCh)
                .str(StrKey::StorageClass, "sc")
                .u64(U64Key::CopyNumber, 2)
        );

        let cmd = sent(|c| async move { c.storage_classes().delete("sc").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Storageclass, SubCmd::SubcmdRm).str(StrKey::StorageClass, "sc")
        );
    }
}

mod user_mount_rule {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<RequesterMountRuleLsItem>(cta_admin_jsonl!("user_mount_rules.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::RmrlsItem));
        let (rule, cmd) = run(reply.clone(), |c| async move {
            c.user_mount_rules().get("atlas003", "eosctaatlaspps").await
        })
        .await;
        assert_eq!(rule.user_name, "atlas003");
        assert_eq!(rule.mount_policy_name, "ctaatlaspps");
        assert_eq!(cmd, Sent::new(Cmd::Requestermountrule, SubCmd::SubcmdLs));
        // the disk instance is part of the key
        let error = failure(reply, |c| async move {
            c.user_mount_rules()
                .get("atlas003", "eosctapublicpps")
                .await
        })
        .await;
        assert!(matches!(error, AdminError::NotFound { .. }), "{error}");

        let cmd = listed(|c| async move { c.user_mount_rules().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Requestermountrule, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move {
            c.user_mount_rules()
                .create("u", "eos")
                .mount_policy_name("mp")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Requestermountrule, SubCmd::SubcmdAdd)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
                .str(StrKey::MountPolicy, "mp")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move {
            c.user_mount_rules()
                .update("u", "eos")
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Requestermountrule, SubCmd::SubcmdCh)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
                .str(StrKey::Comment, "c2")
        );

        let cmd = sent(|c| async move { c.user_mount_rules().delete("u", "eos").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Requestermountrule, SubCmd::SubcmdRm)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
        );
    }
}

mod group_mount_rule {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<GroupMountRuleLsItem>(cta_admin_jsonl!("group_mount_rules.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::GmrlsItem));
        let (rule, cmd) = run(reply.clone(), |c| async move {
            c.group_mount_rules().get("za", "eosctapublicpps").await
        })
        .await;
        assert_eq!(rule.group_name, "za");
        assert_eq!(rule.metadata.comment, "nTOF group");
        assert_eq!(cmd, Sent::new(Cmd::Groupmountrule, SubCmd::SubcmdLs));
        let error = failure(reply, |c| async move {
            c.group_mount_rules().get("za", "eosctaatlaspps").await
        })
        .await;
        assert!(matches!(error, AdminError::NotFound { .. }), "{error}");

        let cmd = listed(|c| async move { c.group_mount_rules().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Groupmountrule, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move {
            c.group_mount_rules()
                .create("g", "eos")
                .mount_policy_name("mp")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Groupmountrule, SubCmd::SubcmdAdd)
                .str(StrKey::Username, "g")
                .str(StrKey::Instance, "eos")
                .str(StrKey::MountPolicy, "mp")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move {
            c.group_mount_rules()
                .update("g", "eos")
                .mount_policy_name("mp2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Groupmountrule, SubCmd::SubcmdCh)
                .str(StrKey::Username, "g")
                .str(StrKey::Instance, "eos")
                .str(StrKey::MountPolicy, "mp2")
        );

        let cmd = sent(|c| async move { c.group_mount_rules().delete("g", "eos").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Groupmountrule, SubCmd::SubcmdRm)
                .str(StrKey::Username, "g")
                .str(StrKey::Instance, "eos")
        );
    }
}

mod activity_mount_rule {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records =
            parse::<ActivityMountRuleLsItem>(cta_admin_jsonl!("activity_mount_rules.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::AmrlsItem));
        let (rule, cmd) = run(reply.clone(), |c| async move {
            c.activity_mount_rules()
                .get("ctaops", "eosctaatlasother", "^express$")
                .await
        })
        .await;
        assert_eq!(
            (
                rule.user_name.as_str(),
                rule.disk_instance_name.as_str(),
                rule.regex.as_str()
            ),
            ("ctaops", "eosctaatlasother", "^express$")
        );
        assert_eq!(cmd, Sent::new(Cmd::Activitymountrule, SubCmd::SubcmdLs));
        // the disk instance and the regex are part of the key
        let error = failure(reply, |c| async move {
            c.activity_mount_rules()
                .get("ctaops", "eosctaatlasother", "^Julien is Staging$")
                .await
        })
        .await;
        assert!(matches!(error, AdminError::NotFound { .. }), "{error}");

        let cmd = listed(|c| async move { c.activity_mount_rules().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Activitymountrule, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move {
            c.activity_mount_rules()
                .create("u", "eos", "^x$")
                .mount_policy_name("mp")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Activitymountrule, SubCmd::SubcmdAdd)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
                .str(StrKey::ActivityRegex, "^x$")
                .str(StrKey::MountPolicy, "mp")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move {
            c.activity_mount_rules()
                .update("u", "eos", "^x$")
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Activitymountrule, SubCmd::SubcmdCh)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
                .str(StrKey::ActivityRegex, "^x$")
                .str(StrKey::Comment, "c2")
        );

        let cmd =
            sent(|c| async move { c.activity_mount_rules().delete("u", "eos", "^x$").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Activitymountrule, SubCmd::SubcmdRm)
                .str(StrKey::Username, "u")
                .str(StrKey::Instance, "eos")
                .str(StrKey::ActivityRegex, "^x$")
        );
    }
}
