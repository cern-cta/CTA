// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of disk instances, disk instance spaces and disk systems against the mock frontend (see
//! [`crate::admin::dao::tests`]).

use std::time::Duration;

use cta_protobuf::cta::{
    admin::{
        DiskInstanceLsItem, DiskInstanceSpaceLsItem, DiskSystemLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};

use crate::admin::{
    AdminError,
    dao::tests::{Sent, failure, listed, run, sent},
    test_support::{Reply, cta_admin_jsonl, parse},
};

mod instance {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<DiskInstanceLsItem>(cta_admin_jsonl!("disk_instances.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::DilsItem));
        let (instance, cmd) = run(reply, |c| async move {
            c.disk_instances().get("eosctalhcbpps").await
        })
        .await;
        assert_eq!(instance.name, "eosctalhcbpps");
        assert_eq!(cmd, Sent::new(Cmd::Diskinstance, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.disk_instances().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Diskinstance, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move { c.disk_instances().create("eos", "c").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstance, SubCmd::SubcmdAdd)
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move { c.disk_instances().update("eos", "c2").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstance, SubCmd::SubcmdCh)
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::Comment, "c2")
        );

        let cmd = sent(|c| async move { c.disk_instances().delete("eos").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstance, SubCmd::SubcmdRm).str(StrKey::DiskInstance, "eos")
        );
    }
}

mod instance_space {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records =
            parse::<DiskInstanceSpaceLsItem>(cta_admin_jsonl!("disk_instance_spaces.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::DislsItem));
        let (space, cmd) = run(reply.clone(), |c| async move {
            c.disk_instance_spaces()
                .get("eosctapublic_retrieve_space", "eosctapublic")
                .await
        })
        .await;
        assert_eq!(
            (space.name.as_str(), space.disk_instance_name.as_str()),
            ("eosctapublic_retrieve_space", "eosctapublic")
        );
        assert_eq!(cmd, Sent::new(Cmd::Diskinstancespace, SubCmd::SubcmdLs));
        // the disk instance is part of the key
        let error = failure(reply, |c| async move {
            c.disk_instance_spaces()
                .get("eosctapublic_retrieve_space", "eosctapublicpps")
                .await
        })
        .await;
        assert!(matches!(error, AdminError::NotFound { .. }), "{error}");

        let cmd = listed(|c| async move { c.disk_instance_spaces().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Diskinstancespace, SubCmd::SubcmdLs));

        // the default refresh interval
        let cmd = sent(|c| async move {
            c.disk_instance_spaces()
                .create("space", "eos")
                .free_space_url("eos:ctaeos:default")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstancespace, SubCmd::SubcmdAdd)
                .str(StrKey::DiskInstanceSpace, "space")
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::FreeSpaceQueryUrl, "eos:ctaeos:default")
                .str(StrKey::Comment, "c")
                .u64(U64Key::RefreshInterval, 10)
        );

        let cmd = sent(|c| async move {
            c.disk_instance_spaces()
                .update("space", "eos")
                .free_space_url("eos:ctaeos:other")
                .refresh_interval(Duration::from_secs(60))
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstancespace, SubCmd::SubcmdCh)
                .str(StrKey::DiskInstanceSpace, "space")
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::FreeSpaceQueryUrl, "eos:ctaeos:other")
                .str(StrKey::Comment, "c2")
                .u64(U64Key::RefreshInterval, 60)
        );

        let cmd =
            sent(|c| async move { c.disk_instance_spaces().delete("space", "eos").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Diskinstancespace, SubCmd::SubcmdRm)
                .str(StrKey::DiskInstanceSpace, "space")
                .str(StrKey::DiskInstance, "eos")
        );
    }
}

mod system {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<DiskSystemLsItem>(cta_admin_jsonl!("disk_systems.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::DslsItem));
        let (system, cmd) = run(reply, |c| async move {
            c.disk_systems().get("digital_memory_retrieve").await
        })
        .await;
        assert_eq!(system.name, "digital_memory_retrieve");
        assert_eq!(cmd, Sent::new(Cmd::Disksystem, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.disk_systems().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Disksystem, SubCmd::SubcmdLs));

        // the default free space and sleep time
        let cmd = sent(|c| async move {
            c.disk_systems()
                .create("ds")
                .disk_instance_name("eos")
                .disk_instance_space_name("space")
                .file_regex("^root://eos/")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Disksystem, SubCmd::SubcmdAdd)
                .str(StrKey::DiskSystem, "ds")
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::DiskInstanceSpace, "space")
                .str(StrKey::FileRegexp, "^root://eos/")
                .str(StrKey::Comment, "c")
                .u64(U64Key::TargetedFreeSpace, 1_000_000_000_000)
                .u64(U64Key::SleepTime, 3600)
        );

        let cmd = sent(|c| async move {
            c.disk_systems()
                .update("ds")
                .file_regex("^root://other/")
                .desired_min_free_space(5)
                .sleep_time(Duration::from_secs(30))
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Disksystem, SubCmd::SubcmdCh)
                .str(StrKey::DiskSystem, "ds")
                .str(StrKey::FileRegexp, "^root://other/")
                .str(StrKey::Comment, "c2")
                .u64(U64Key::TargetedFreeSpace, 5)
                .u64(U64Key::SleepTime, 30)
        );

        let cmd = sent(|c| async move { c.disk_systems().delete("ds").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Disksystem, SubCmd::SubcmdRm).str(StrKey::DiskSystem, "ds")
        );
    }
}
