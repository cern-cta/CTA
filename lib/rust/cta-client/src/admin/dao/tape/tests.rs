// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of tapes, tape pools and media types against the mock frontend (see
//! [`crate::admin::dao::tests`]).

use cta_model::tape::TapeState;
use cta_protobuf::cta::{
    admin::{
        MediaTypeLsItem, TapePoolLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_boolean::Key as BoolKey,
        option_str_list::Key as StrListKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};
use tokio_stream::StreamExt;

use crate::admin::{
    AdminError,
    dao::{
        IdFileError,
        tests::{
            Sent, bool_options, listed, pair, run, sent, str_list_options, str_options, tempdir,
            u64_options,
        },
    },
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

#[tokio::test]
async fn list_without_filter_asks_for_all() {
    let (client, calls) = serve(Reply::records([])).await;

    let _tapes = client.tapes().list().call().await.unwrap();

    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Tape, SubCmd::SubcmdLs));
    assert_eq!(bool_options(&cmd), [(BoolKey::All, true)]);
}

#[tokio::test]
async fn list_with_filters_does_not_ask_for_all() {
    let (client, calls) = serve(Reply::records([])).await;

    let _tapes = client
        .tapes()
        .list()
        .state(TapeState::RepackingDisabled)
        .full(false)
        .call()
        .await
        .unwrap();

    let cmd = single_call(&calls).cmd;
    assert_eq!(str_options(&cmd), [(StrKey::State, "REPACKING_DISABLED")]);
    assert_eq!(bool_options(&cmd), [(BoolKey::Full, false)]);
}

#[tokio::test]
async fn list_with_an_id_file_reads_it() {
    let dir = tempdir();
    let path = dir.join("fids.txt");
    std::fs::write(
        &path,
        "path=/eos/a fid=0x1a # a comment\n\n   path=/eos/b   fid=ff\nnot an id\n",
    )
    .unwrap();
    let (client, calls) = serve(Reply::records([])).await;

    let _tapes = client
        .tapes()
        .list()
        .disk_file_ids_file(&path)
        .disk_file_ids(vec!["7".to_owned()])
        .call()
        .await
        .unwrap();

    let cmd = single_call(&calls).cmd;
    assert_eq!(
        str_list_options(&cmd),
        [(
            StrListKey::FileId,
            vec!["7".to_owned(), "26".to_owned(), "255".to_owned()]
        )]
    );
    assert!(bool_options(&cmd).is_empty());
}

#[tokio::test]
async fn list_with_an_unreadable_id_file_fails() {
    let (client, calls) = serve(Reply::records([])).await;

    let Err(error) = client
        .tapes()
        .list()
        .disk_file_ids_file("/nonexistent/fids.txt")
        .call()
        .await
    else {
        panic!("an unreadable file should fail");
    };

    assert!(
        matches!(
            error,
            AdminError::IdFile {
                source: IdFileError::Io(_),
                ..
            }
        ),
        "{error}"
    );
    assert!(calls.lock().unwrap().is_empty(), "nothing should be sent");
}

#[tokio::test]
async fn change_state() {
    let (client, calls) = serve(Reply::success()).await;

    client
        .tapes()
        .change_state("I71007", TapeState::RepackingDisabled, "repack")
        .await
        .unwrap();

    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Tape, SubCmd::SubcmdCh));
    assert_eq!(
        str_options(&cmd),
        [
            (StrKey::Vid, "I71007"),
            (StrKey::State, "REPACKING_DISABLED"),
            (StrKey::Reason, "repack")
        ]
    );
}

#[tokio::test]
async fn update_with_verification_status() {
    let (client, calls) = serve(Reply::success()).await;

    client
        .tapes()
        .update("I71007")
        .verification_status(cta_model::tape::VerificationStatus {
            date: jiff::Timestamp::from_second(1_735_689_600).unwrap(),
            status: "finished".into(),
            file_submitted_count: 10,
            file_verified_count: 0,
            file_failed_count: 0,
        })
        .dirty(false)
        .comment("new_comment")
        .call()
        .await
        .unwrap();

    let cmd = single_call(&calls).cmd;
    let options = str_options(&cmd);
    assert_eq!(options.len(), 3);
    assert_eq!(options[0], (StrKey::Vid, "I71007"));
    assert_eq!(options[1].0, StrKey::VerificationStatus);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(options[1].1).unwrap()["files_submitted"],
        10
    );
    assert_eq!(options[2], (StrKey::Comment, "new_comment"));
    assert_eq!(bool_options(&cmd), [(BoolKey::DirtyBit, false)]);
}

#[tokio::test]
async fn create() {
    let (client, calls) = serve(Reply::success()).await;

    client
        .tapes()
        .create("V01001")
        .media_type_name("LTO9")
        .vendor("IBM")
        .logical_library_name("lib")
        .tape_pool_name("pool")
        .comment("c")
        .call()
        .await
        .unwrap();

    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Tape, SubCmd::SubcmdAdd));
    assert_eq!(
        str_options(&cmd),
        [
            (StrKey::Vid, "V01001"),
            (StrKey::MediaType, "LTO9"),
            (StrKey::Vendor, "IBM"),
            (StrKey::LogicalLibrary, "lib"),
            (StrKey::TapePool, "pool"),
            (StrKey::Comment, "c"),
        ]
    );
    assert_eq!(bool_options(&cmd), [(BoolKey::Full, false)]);
}

#[tokio::test]
async fn sends_the_commands() {
    // every filter, none of which is `--all`
    let cmd = listed(|c| async move {
        c.tapes()
            .list()
            .media_type_name("LTO9")
            .vendor("IBM")
            .logical_library_name("lib")
            .tape_pool_name("tp")
            .virtual_organisation_name("vo")
            .full(true)
            .disk_file_ids(vec!["1".into(), "2".into()])
            .state(TapeState::Active)
            .purchase_order("po")
            .physical_library_name("phys")
            .missing_file_copies(true)
            .call()
            .await
    })
    .await;
    assert_eq!(
        cmd,
        Sent::new(Cmd::Tape, SubCmd::SubcmdLs)
            .str(StrKey::MediaType, "LTO9")
            .str(StrKey::Vendor, "IBM")
            .str(StrKey::LogicalLibrary, "lib")
            .str(StrKey::TapePool, "tp")
            .str(StrKey::Vo, "vo")
            .str(StrKey::State, "ACTIVE")
            .str(StrKey::MediaPurchaseOrderNumber, "po")
            .str(StrKey::PhysicalLibrary, "phys")
            .bool(BoolKey::Full, true)
            .bool(BoolKey::MissingFileCopies, true)
            .str_list(StrListKey::FileId, &["1", "2"])
    );

    let cmd = sent(|c| async move { c.tapes().reclaim("V01007").await }).await;
    assert_eq!(
        cmd,
        Sent::new(Cmd::Tape, SubCmd::SubcmdReclaim).str(StrKey::Vid, "V01007")
    );

    let cmd = sent(|c| async move { c.tapes().delete("V01007").await }).await;
    assert_eq!(
        cmd,
        Sent::new(Cmd::Tape, SubCmd::SubcmdRm).str(StrKey::Vid, "V01007")
    );
}

mod pool {
    use super::*;

    #[tokio::test]
    async fn list_converts_every_record() {
        let records = parse::<TapePoolLsItem>(cta_admin_jsonl!("tape_pools.jsonl"));
        let (client, calls) = serve(Reply::records(records.into_iter().map(Data::TplsItem))).await;

        let pools: Vec<_> = client
            .tape_pools()
            .list()
            .virtual_organisation_name("IT")
            .encrypted(true)
            .call()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .await
            .unwrap();

        assert_eq!(
            pools.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["atlaspps", "cbacktest", "erase_alltypes"]
        );
        let cmd = single_call(&calls).cmd;
        assert_eq!(str_options(&cmd), [(StrKey::Vo, "IT")]);
        assert_eq!(bool_options(&cmd), [(BoolKey::Encrypted, true)]);
    }

    #[tokio::test]
    async fn create_joins_the_supply_pools() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .tape_pools()
            .create("pool")
            .virtual_organisation_name("IT")
            .supply_tape_pool_names(vec!["a".into(), "b".into()])
            .comment("c")
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(
            str_options(&cmd),
            [
                (StrKey::TapePool, "pool"),
                (StrKey::Vo, "IT"),
                (StrKey::Supply, "a,b"),
                (StrKey::Comment, "c"),
            ]
        );
        assert_eq!(u64_options(&cmd), [(U64Key::PartialTapesNumber, 1)]);
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let cmd = sent(|c| async move {
            c.tape_pools()
                .update("tp")
                .virtual_organisation_name("vo")
                .min_writable_tape_count(3)
                .supply_tape_pool_names(vec!["a".into(), "b".into()])
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Tapepool, SubCmd::SubcmdCh)
                .str(StrKey::TapePool, "tp")
                .str(StrKey::Vo, "vo")
                .str(StrKey::Supply, "a,b")
                .str(StrKey::Comment, "c")
                .u64(U64Key::PartialTapesNumber, 3)
        );

        let cmd =
            sent(|c| async move { c.tape_pools().set_encryption_key_name("tp", "key").await })
                .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Tapepool, SubCmd::SubcmdCh)
                .str(StrKey::TapePool, "tp")
                .str(StrKey::EncryptionKeyName, "key")
        );

        // an empty key name removes the encryption
        let cmd = sent(|c| async move { c.tape_pools().remove_encryption("tp").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Tapepool, SubCmd::SubcmdCh)
                .str(StrKey::TapePool, "tp")
                .str(StrKey::EncryptionKeyName, "")
        );

        let cmd = sent(|c| async move { c.tape_pools().delete("tp").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Tapepool, SubCmd::SubcmdRm).str(StrKey::TapePool, "tp")
        );
    }
}

mod media_type {
    use super::*;

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<MediaTypeLsItem>(cta_admin_jsonl!("media_types.jsonl"));
        let reply = Reply::records(records.into_iter().map(Data::MtlsItem));
        let (media_type, cmd) = run(
            reply,
            |c| async move { c.media_types().get("3592JD15T").await },
        )
        .await;
        assert_eq!(media_type.name, "3592JD15T");
        assert_eq!(cmd, Sent::new(Cmd::Mediatype, SubCmd::SubcmdLs));

        let cmd = listed(|c| async move { c.media_types().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Mediatype, SubCmd::SubcmdLs));

        // the default density codes
        let cmd = sent(|c| async move {
            c.media_types()
                .create("LTO9")
                .cartridge_type("LTO-9")
                .capacity(18_000_000_000_000)
                .comment("c")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Mediatype, SubCmd::SubcmdAdd)
                .str(StrKey::MediaType, "LTO9")
                .str(StrKey::Cartridge, "LTO-9")
                .str(StrKey::Comment, "c")
                .u64(U64Key::Capacity, 18_000_000_000_000)
                .u64(U64Key::PrimaryDensityCode, 0)
                .u64(U64Key::SecondaryDensityCode, 0)
        );

        let cmd = sent(|c| async move {
            c.media_types()
                .update("LTO9")
                .cartridge_type("LTO-9 WORM")
                .primary_density_code(96)
                .secondary_density_code(97)
                .comment("c2")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Mediatype, SubCmd::SubcmdCh)
                .str(StrKey::MediaType, "LTO9")
                .str(StrKey::Cartridge, "LTO-9 WORM")
                .str(StrKey::Comment, "c2")
                .u64(U64Key::PrimaryDensityCode, 96)
                .u64(U64Key::SecondaryDensityCode, 97)
        );

        let cmd = sent(|c| async move { c.media_types().delete("LTO9").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Mediatype, SubCmd::SubcmdRm).str(StrKey::MediaType, "LTO9")
        );
    }
}
