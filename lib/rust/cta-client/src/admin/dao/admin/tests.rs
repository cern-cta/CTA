// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of admin users and virtual organisations against the mock frontend (see
//! [`crate::admin::dao::tests`]).

use cta_protobuf::cta::{
    admin::{
        VirtualOrganizationLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_boolean::Key as BoolKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};

use crate::admin::{
    dao::tests::{Sent, bool_options, listed, run, sent, u64_options},
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

mod virtual_organisation {
    use super::*;

    #[tokio::test]
    async fn create_sends_booleans() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .virtual_organisations()
            .create("IT")
            .disk_instance_name("eosctarepack")
            .is_repack_vo(true)
            .comment("c")
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(bool_options(&cmd), [(BoolKey::IsRepackVo, true)]);
        assert_eq!(
            u64_options(&cmd),
            [
                (U64Key::ReadMaxDrives, 1),
                (U64Key::WriteMaxDrives, 1),
                (U64Key::MaxFileSize, 1_000_000_000_000)
            ]
        );
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records =
            parse::<VirtualOrganizationLsItem>(cta_admin_jsonl!("virtual_organisations.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::VolsItem));
        let (vo, cmd) = run(reply, |c| async move {
            c.virtual_organisations().get("IT").await
        })
        .await;
        assert_eq!(vo.name, "IT");
        assert_eq!(cmd, Sent::new(Cmd::Virtualorganization, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.virtual_organisations().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Virtualorganization, SubCmd::SubcmdLs));

        // the defaults
        let cmd = sent(|c| async move {
            c.virtual_organisations()
                .create("vo")
                .disk_instance_name("eos")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Virtualorganization, SubCmd::SubcmdAdd)
                .str(StrKey::Vo, "vo")
                .str(StrKey::DiskInstance, "eos")
                .str(StrKey::Comment, "c")
                .u64(U64Key::ReadMaxDrives, 1)
                .u64(U64Key::WriteMaxDrives, 1)
                .u64(U64Key::MaxFileSize, 1_000_000_000_000)
                .bool(BoolKey::IsRepackVo, false)
        );

        let cmd = sent(|c| async move {
            c.virtual_organisations()
                .update("vo")
                .disk_instance_name("eos2")
                .is_repack_vo(true)
                .max_read_drives(2)
                .max_write_drives(3)
                .max_file_size(4)
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Virtualorganization, SubCmd::SubcmdCh)
                .str(StrKey::Vo, "vo")
                .str(StrKey::DiskInstance, "eos2")
                .str(StrKey::Comment, "c2")
                .u64(U64Key::ReadMaxDrives, 2)
                .u64(U64Key::WriteMaxDrives, 3)
                .u64(U64Key::MaxFileSize, 4)
                .bool(BoolKey::IsRepackVo, true)
        );

        let cmd = sent(|c| async move { c.virtual_organisations().delete("vo").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Virtualorganization, SubCmd::SubcmdRm).str(StrKey::Vo, "vo")
        );
    }
}

mod admin_user {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let cmd = listed(|c| async move { c.admin_users().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Admin, SubCmd::SubcmdLs));

        let cmd =
            sent(|c| async move { c.admin_users().create("afonso", "new admin").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Admin, SubCmd::SubcmdAdd)
                .str(StrKey::Username, "afonso")
                .str(StrKey::Comment, "new admin")
        );

        let cmd =
            sent(|c| async move { c.admin_users().update("afonso", "still admin").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Admin, SubCmd::SubcmdCh)
                .str(StrKey::Username, "afonso")
                .str(StrKey::Comment, "still admin")
        );
    }
}
