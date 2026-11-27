// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of files and of the recycle bin against the mock frontend (see
//! [`crate::admin::dao::tests`]).

use cta_model::file::FileId;
use cta_protobuf::cta::{
    admin::{
        RecycleTapeFileLsItem, TapeFileLsItem,
        admin_cmd::{Cmd, SubCmd},
        option_str_list::Key as StrListKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};
use tokio_stream::StreamExt;

use crate::admin::{
    AdminError,
    dao::tests::{Sent, failure, listed, pair, run, str_options, tempdir, u64_options},
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

#[tokio::test]
async fn get_by_disk_file_id_and_vid() {
    let records = parse::<TapeFileLsItem>(cta_admin_jsonl!("tape_files.jsonl"));
    let (client, calls) = serve(Reply::records([Data::TflsItem(records[0].clone())])).await;

    let file = client
        .tape_files()
        .get(FileId::disk("4296353178", "eosctapublicpps"), "I72052")
        .await
        .unwrap();

    assert_eq!(file.archive_file.archive_id, 4_303_580_667);
    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Tapefile, SubCmd::SubcmdLs));
    assert_eq!(
        str_options(&cmd),
        [
            (StrKey::Vid, "I72052"),
            (StrKey::Instance, "eosctapublicpps"),
            (StrKey::DiskFileId, "4296353178"),
        ]
    );
    assert!(u64_options(&cmd).is_empty());
}

/// The copy of a file on one tape, which the frontend picks out of the copies of the file.
#[tokio::test]
async fn get_by_archive_id_and_vid() {
    let records = parse::<TapeFileLsItem>(cta_admin_jsonl!("tape_files.jsonl"));
    let (client, calls) = serve(Reply::records([Data::TflsItem(records[0].clone())])).await;

    let file = client
        .tape_files()
        .get(4_303_580_667, "I72052")
        .await
        .unwrap();

    assert_eq!(file.archive_file.archive_id, 4_303_580_667);
    assert_eq!(file.tape_file.vid, "I72052");
    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Tapefile, SubCmd::SubcmdLs));
    assert_eq!(str_options(&cmd), [(StrKey::Vid, "I72052")]);
    assert_eq!(u64_options(&cmd), [(U64Key::ArchiveFileId, 4_303_580_667)]);
}

#[tokio::test]
async fn get_all_copies_by_archive_id() {
    let records = parse::<TapeFileLsItem>(cta_admin_jsonl!("tape_files.jsonl"));
    let (client, calls) = serve(Reply::records(
        records[..2].iter().cloned().map(Data::TflsItem),
    ))
    .await;

    let copies = client
        .tape_files()
        .get_all_copies(4_303_580_667)
        .await
        .unwrap();

    assert_eq!(copies.len(), 2);
    assert_eq!(copies[1].tape_file.vid, "I72000");
    let cmd = single_call(&calls).cmd;
    assert_eq!(u64_options(&cmd), [(U64Key::ArchiveFileId, 4_303_580_667)]);
    assert!(str_options(&cmd).is_empty());
}

#[tokio::test]
async fn get_all_copies_of_an_unknown_file_is_not_found() {
    let (client, _) = serve(Reply::records([])).await;

    let error = client.tape_files().get_all_copies(1).await.unwrap_err();

    assert!(matches!(error, AdminError::NotFound { resource: "tape file", ref id } if id == "1"));
}

#[tokio::test]
async fn delete_sends_the_reason() {
    let (client, calls) = serve(Reply::success()).await;

    client
        .tape_files()
        .delete(4_303_580_667, "I72052", "test")
        .await
        .unwrap();

    let call = single_call(&calls);
    assert_eq!(call.rpc, "Admin");
    assert_eq!(pair(&call.cmd), (Cmd::Tapefile, SubCmd::SubcmdRm));
    assert_eq!(
        str_options(&call.cmd),
        [(StrKey::Vid, "I72052"), (StrKey::Reason, "test")]
    );
    assert_eq!(
        u64_options(&call.cmd),
        [(U64Key::ArchiveFileId, 4_303_580_667)]
    );
}

#[tokio::test]
async fn list() {
    let cmd = listed(|c| async move { c.tape_files().list().vid("V01007").call().await }).await;
    assert_eq!(
        cmd,
        Sent::new(Cmd::Tapefile, SubCmd::SubcmdLs).str(StrKey::Vid, "V01007")
    );

    // the IDs given and those of the file are sent together
    let path = tempdir().join("fids");
    std::fs::write(&path, "path=/eos/a fid=1a\n").unwrap();
    let cmd = listed(move |c| async move {
        c.tape_files()
            .list()
            .disk_file_ids(vec!["7".into()])
            .disk_file_ids_file(path)
            .disk_instance_name("eos")
            .call()
            .await
    })
    .await;
    assert_eq!(
        cmd,
        Sent::new(Cmd::Tapefile, SubCmd::SubcmdLs)
            .str(StrKey::Instance, "eos")
            .str_list(StrListKey::FileId, &["7", "26"])
    );
}

mod recycle_bin {
    use super::*;

    #[tokio::test]
    async fn list_and_restore() {
        let records = parse::<cta_protobuf::cta::admin::RecycleTapeFileLsItem>(cta_admin_jsonl!(
            "recycle_bin_tape_files.jsonl"
        ));
        let (client, calls) = serve(Reply::records(records.into_iter().map(Data::RtflsItem))).await;
        let deleted: Vec<_> = client
            .recycle_bin_tape_files()
            .list()
            .vid("L85033")
            .virtual_organisation_name("IT")
            .min_deletion_time(jiff::Timestamp::from_second(946_684_800).unwrap())
            .max_deletion_time(jiff::Timestamp::from_second(1_735_689_600).unwrap())
            .call()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .await
            .unwrap();

        assert_eq!(deleted.len(), 3);
        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Recycletapefile, SubCmd::SubcmdLs));
        assert_eq!(
            str_options(&cmd),
            [(StrKey::Vid, "L85033"), (StrKey::Vo, "IT")]
        );
        assert_eq!(
            u64_options(&cmd),
            [
                (U64Key::LogUnixtimeMin, 946_684_800),
                (U64Key::LogUnixtimeMax, 1_735_689_600)
            ]
        );

        let (client, calls) = serve(Reply::success()).await;
        let message = client
            .recycle_bin_tape_files()
            .restore(&deleted[1].file)
            .await
            .unwrap();

        assert_eq!(message, "done");
        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Recycletapefile, SubCmd::SubcmdRestore));
        assert_eq!(
            str_options(&cmd),
            [
                (StrKey::Vid, "L85033"),
                (StrKey::DiskFileId, "4301523000"),
                (StrKey::Instance, "eosctaatlaspps"),
            ]
        );
        assert_eq!(
            u64_options(&cmd),
            [
                (U64Key::ArchiveFileId, 4_302_486_079),
                (U64Key::CopyNumber, 2)
            ]
        );
    }

    #[tokio::test]
    async fn get() {
        let records =
            parse::<RecycleTapeFileLsItem>(cta_admin_jsonl!("recycle_bin_tape_files.jsonl"));

        let reply = Reply::records([Data::RtflsItem(records[1].clone())]);
        let (deleted, cmd) = run(reply, |c| async move {
            c.recycle_bin_tape_files()
                .get(FileId::disk("4301523000", "eosctaatlaspps"), "L85033")
                .await
        })
        .await;
        assert_eq!(deleted.file.archive_file.archive_id, 4_302_486_079);
        assert_eq!(deleted.file.tape_file.copy_number, 2);
        assert_eq!(
            cmd,
            Sent::new(Cmd::Recycletapefile, SubCmd::SubcmdLs)
                .str(StrKey::Vid, "L85033")
                .str(StrKey::DiskFileId, "4301523000")
                .str(StrKey::Instance, "eosctaatlaspps")
        );

        let reply = Reply::records(records[..2].iter().cloned().map(Data::RtflsItem));
        let (copies, cmd) = run(reply, |c| async move {
            c.recycle_bin_tape_files()
                .get_all_copies(4_302_486_079)
                .await
        })
        .await;
        assert_eq!(
            copies
                .iter()
                .map(|f| f.file.tape_file.copy_number)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            cmd,
            Sent::new(Cmd::Recycletapefile, SubCmd::SubcmdLs)
                .u64(U64Key::ArchiveFileId, 4_302_486_079)
        );

        let error = failure(Reply::records([]), |c| async move {
            c.recycle_bin_tape_files().get_all_copies(1).await
        })
        .await;
        assert!(
            matches!(
                error,
                AdminError::NotFound {
                    resource: "recycle bin tape file",
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn list_filters() {
        let path = tempdir().join("fids");
        std::fs::write(&path, "fid=ff\n").unwrap();
        let cmd = listed(move |c| async move {
            c.recycle_bin_tape_files()
                .list()
                .archive_id(42)
                .copy_number(2)
                .disk_file_ids(vec!["1".into()])
                .disk_file_ids_file(path)
                .disk_instance_name("eos")
                .call()
                .await
        })
        .await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Recycletapefile, SubCmd::SubcmdLs)
                .str(StrKey::Instance, "eos")
                .u64(U64Key::ArchiveFileId, 42)
                .u64(U64Key::CopyNumber, 2)
                .str_list(StrListKey::FileId, &["1", "255"])
        );
    }
}
