// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of drives and libraries against the mock frontend (see [`crate::admin::dao::tests`]).

use cta_model::library::LibraryState;
use cta_protobuf::cta::{
    admin::{
        DriveLsItem, LogicalLibraryLsItem, PhysicalLibraryLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_boolean::Key as BoolKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};
use tokio_stream::StreamExt;

use crate::admin::{
    dao::tests::{Sent, bool_options, listed, pair, run, sent, str_options},
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

mod physical {
    use super::*;

    #[tokio::test]
    async fn filtered_by_state_on_the_client() {
        let records = parse::<cta_protobuf::cta::admin::PhysicalLibraryLsItem>(cta_admin_jsonl!(
            "physical_libraries.jsonl"
        ));
        let (client, _) = serve(Reply::records(records.into_iter().map(Data::PllsItem))).await;

        let disabled: Vec<_> = client
            .physical_libraries()
            .list()
            .state(LibraryState::Disabled)
            .call()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .await
            .unwrap();

        assert_eq!(disabled.len(), 1);
        assert_eq!(disabled[0].name, "IBMLIB3");
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<PhysicalLibraryLsItem>(cta_admin_jsonl!("physical_libraries.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::PllsItem));
        let (library, cmd) = run(reply, |c| async move {
            c.physical_libraries().get("IBMLIB2").await
        })
        .await;
        assert_eq!(library.name, "IBMLIB2");
        assert_eq!(cmd, Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdLs));

        // the required arguments only
        let cmd = sent(|c| async move {
            c.physical_libraries()
                .create("lib")
                .manufacturer("IBM")
                .model("TS4500")
                .physical_cartridge_slots(1000)
                .physical_drive_slots(10)
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdAdd)
                .str(StrKey::PhysicalLibrary, "lib")
                .str(StrKey::Manufacturer, "IBM")
                .str(StrKey::LibraryModel, "TS4500")
                .u64(U64Key::NbPhysicalCartridgeSlots, 1000)
                .u64(U64Key::NbPhysicalDriveSlots, 10)
        );
        // all of them
        let cmd = sent(|c| async move {
            c.physical_libraries()
                .create("lib")
                .manufacturer("IBM")
                .model("TS4500")
                .physical_cartridge_slots(1000)
                .physical_drive_slots(10)
                .gui_url("https://gui")
                .webcam_url("https://webcam")
                .library_type("tape")
                .location("513")
                .available_cartridge_slots(900)
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdAdd)
                .str(StrKey::PhysicalLibrary, "lib")
                .str(StrKey::Manufacturer, "IBM")
                .str(StrKey::LibraryModel, "TS4500")
                .str(StrKey::GuiUrl, "https://gui")
                .str(StrKey::WebcamUrl, "https://webcam")
                .str(StrKey::LibraryType, "tape")
                .str(StrKey::LibraryLocation, "513")
                .str(StrKey::Comment, "c")
                .u64(U64Key::NbPhysicalCartridgeSlots, 1000)
                .u64(U64Key::NbAvailableCartridgeSlots, 900)
                .u64(U64Key::NbPhysicalDriveSlots, 10)
        );

        let cmd = sent(|c| async move {
            c.physical_libraries()
                .update("lib")
                .gui_url("https://gui2")
                .webcam_url("https://webcam2")
                .location("613")
                .physical_cartridge_slots(1)
                .available_cartridge_slots(2)
                .physical_drive_slots(3)
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdCh)
                .str(StrKey::PhysicalLibrary, "lib")
                .str(StrKey::GuiUrl, "https://gui2")
                .str(StrKey::WebcamUrl, "https://webcam2")
                .str(StrKey::LibraryLocation, "613")
                .str(StrKey::Comment, "c2")
                .u64(U64Key::NbPhysicalCartridgeSlots, 1)
                .u64(U64Key::NbAvailableCartridgeSlots, 2)
                .u64(U64Key::NbPhysicalDriveSlots, 3)
        );

        let cmd =
            sent(|c| async move { c.physical_libraries().disable("lib", "maintenance").await })
                .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdCh)
                .str(StrKey::PhysicalLibrary, "lib")
                .str(StrKey::DisabledReason, "maintenance")
                .bool(BoolKey::Disabled, true)
        );

        let cmd = sent(|c| async move { c.physical_libraries().enable("lib").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdCh)
                .str(StrKey::PhysicalLibrary, "lib")
                .bool(BoolKey::Disabled, false)
        );

        let cmd = sent(|c| async move { c.physical_libraries().delete("lib").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Physicallibrary, SubCmd::SubcmdRm).str(StrKey::PhysicalLibrary, "lib")
        );
    }
}

mod drive {
    use super::*;

    #[tokio::test]
    async fn down_with_force() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .drives()
            .down("DRIVE1")
            .reason("broken")
            .force(true)
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Drive, SubCmd::SubcmdDown));
        assert_eq!(
            str_options(&cmd),
            [(StrKey::Drive, "DRIVE1"), (StrKey::Reason, "broken")]
        );
        assert_eq!(bool_options(&cmd), [(BoolKey::Force, true)]);
    }

    #[tokio::test]
    async fn up_without_force_flag() {
        let (client, calls) = serve(Reply::success()).await;

        client.drives().up("DRIVE1").call().await.unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Drive, SubCmd::SubcmdUp));
        assert_eq!(str_options(&cmd), [(StrKey::Drive, "DRIVE1")]);
        assert!(bool_options(&cmd).is_empty());
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<DriveLsItem>(cta_admin_jsonl!("drives.jsonl"));
        let reply = Reply::records([Data::DrlsItem(records[1].clone())]);
        let (drive, cmd) = run(reply, |c| async move {
            c.drives().get("SPECTRALIB3-LTO9-F10B1S2").await
        })
        .await;
        assert_eq!(drive.name, "SPECTRALIB3-LTO9-F10B1S2");
        assert_eq!(
            cmd,
            Sent::new(Cmd::Drive, SubCmd::SubcmdLs).str(StrKey::Drive, "SPECTRALIB3-LTO9-F10B1S2")
        );

        let cmd = listed(|c| async move { c.drives().list().call().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Drive, SubCmd::SubcmdLs));
        let cmd = listed(|c| async move { c.drives().list().all(true).call().await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Drive, SubCmd::SubcmdLs).bool(BoolKey::All, true)
        );

        let cmd = sent(|c| async move { c.drives().update("drive", "c").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Drive, SubCmd::SubcmdCh)
                .str(StrKey::Drive, "drive")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move { c.drives().delete("drive").call().await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Drive, SubCmd::SubcmdRm).str(StrKey::Drive, "drive")
        );
        let cmd =
            sent(|c| async move { c.drives().delete("drive").force(true).call().await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Drive, SubCmd::SubcmdRm)
                .str(StrKey::Drive, "drive")
                .bool(BoolKey::Force, true)
        );
    }
}

mod logical {
    use super::*;

    #[tokio::test]
    async fn disable_and_enable() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .logical_libraries()
            .disable("IBM460", "test disable feature")
            .await
            .unwrap();
        client.logical_libraries().enable("IBM460").await.unwrap();

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(pair(&calls[0].cmd), (Cmd::Logicallibrary, SubCmd::SubcmdCh));
        assert_eq!(bool_options(&calls[0].cmd), [(BoolKey::Disabled, true)]);
        assert_eq!(
            str_options(&calls[0].cmd),
            [
                (StrKey::LogicalLibrary, "IBM460"),
                (StrKey::DisabledReason, "test disable feature")
            ]
        );
        assert_eq!(bool_options(&calls[1].cmd), [(BoolKey::Disabled, false)]);
    }

    #[tokio::test]
    async fn create_disabled_by_default() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .logical_libraries()
            .create("lib")
            .physical_library_name("IBMLIB1")
            .comment("c")
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(bool_options(&cmd), [(BoolKey::Disabled, true)]);
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<LogicalLibraryLsItem>(cta_admin_jsonl!("logical_libraries.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::LllsItem));
        let (library, cmd) = run(reply, |c| async move {
            c.logical_libraries().get("IBM460").await
        })
        .await;
        assert_eq!(library.name, "IBM460");
        assert_eq!(library.state, LibraryState::Disabled);
        assert_eq!(cmd, Sent::new(Cmd::Logicallibrary, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.logical_libraries().list().call().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Logicallibrary, SubCmd::SubcmdLs));
        for (state, disabled) in [
            (LibraryState::Disabled, true),
            (LibraryState::Enabled, false),
        ] {
            let cmd =
                listed(
                    move |c| async move { c.logical_libraries().list().state(state).call().await },
                )
                .await;
            assert_eq!(
                cmd,
                Sent::new(Cmd::Logicallibrary, SubCmd::SubcmdLs).bool(BoolKey::Disabled, disabled)
            );
        }

        let cmd = sent(|c| async move {
            c.logical_libraries()
                .update("lib")
                .physical_library_name("phys")
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Logicallibrary, SubCmd::SubcmdCh)
                .str(StrKey::LogicalLibrary, "lib")
                .str(StrKey::PhysicalLibrary, "phys")
                .str(StrKey::Comment, "c")
        );

        let cmd = sent(|c| async move { c.logical_libraries().delete("lib").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Logicallibrary, SubCmd::SubcmdRm).str(StrKey::LogicalLibrary, "lib")
        );
    }
}
