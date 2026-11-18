// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The data access objects of the scheduler requests against the mock frontend (see [`crate::admin::dao::tests`]).

use cta_model::scheduler::{MountType, RepackState};
use cta_protobuf::cta::{
    admin::{
        FailedRequestLsSummary, RepackLsItem, RequestType, ShowQueuesItem,
        admin_cmd::{Cmd, SubCmd},
        option_boolean::Key as BoolKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};
use tokio_stream::StreamExt;

use crate::admin::{
    dao::tests::{Sent, bool_options, listed, pair, run, sent, str_options, u64_options},
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

mod user_request {
    use super::*;

    #[tokio::test]
    async fn filtered_by_direction() {
        let records = parse::<ShowQueuesItem>(cta_admin_jsonl!("requests.jsonl"));
        let (client, _) = serve(Reply::records(records.into_iter().map(Data::SqItem))).await;

        let retrieves: Vec<_> = client
            .user_requests()
            .list()
            .mount_type(MountType::Retrieve)
            .call()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .await
            .unwrap();
        let archives: Vec<_> = client
            .user_requests()
            .list()
            .mount_type(MountType::Archive)
            .call()
            .await
            .unwrap()
            .collect::<Result<_, _>>()
            .await
            .unwrap();

        assert!(retrieves.is_empty());
        assert_eq!(archives.len(), 3);
    }
}

mod failed_request {
    use super::*;

    #[tokio::test]
    async fn summary_drops_the_total() {
        let summary = |request_type: RequestType, total_files| FailedRequestLsSummary {
            request_type: request_type.into(),
            total_files,
            total_size: total_files * 10,
            scheduler_backend_name: "cephUser".into(),
            ..Default::default()
        };
        let (client, calls) = serve(Reply::records([
            Data::FrlsSummary(summary(RequestType::ArchiveRequest, 0)),
            Data::FrlsSummary(summary(RequestType::RetrieveRequest, 51_998)),
            Data::FrlsSummary(summary(RequestType::Total, 51_998)),
        ]))
        .await;

        let summaries = client.failed_requests().summary().await.unwrap();

        assert_eq!(
            summaries
                .iter()
                .map(|s| (s.mount_type, s.total_file_count))
                .collect::<Vec<_>>(),
            [(MountType::Archive, 0), (MountType::Retrieve, 51_998)]
        );
        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Failedrequest, SubCmd::SubcmdLs));
        assert_eq!(bool_options(&cmd), [(BoolKey::Summary, true)]);
    }

    #[tokio::test]
    async fn list_filters() {
        let (client, calls) = serve(Reply::records([])).await;

        let _requests = client
            .failed_requests()
            .list()
            .mount_type(MountType::Retrieve)
            .logs(true)
            .vid("V01001")
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(
            bool_options(&cmd),
            [
                (BoolKey::Justretrieve, true),
                (BoolKey::ShowLogEntries, true)
            ]
        );
        assert_eq!(str_options(&cmd), [(StrKey::Vid, "V01001")]);
    }

    #[tokio::test]
    async fn delete() {
        let cmd =
            sent(|c| async move { c.failed_requests().delete("ArchiveRequest-123").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Failedrequest, SubCmd::SubcmdRm)
                .str(StrKey::Objectid, "ArchiveRequest-123")
        );
    }
}

mod repack_request {
    use super::*;

    #[tokio::test]
    async fn create_with_operation() {
        let (client, calls) = serve(Reply::success()).await;

        client
            .repack_requests()
            .create("V01001")
            .mount_policy_name("mp")
            .no_recall(true)
            .max_files_to_select(10)
            .operation(cta_model::scheduler::RepackOperation::Move)
            .call()
            .await
            .unwrap();

        let cmd = single_call(&calls).cmd;
        assert_eq!(pair(&cmd), (Cmd::Repack, SubCmd::SubcmdAdd));
        assert_eq!(
            str_options(&cmd),
            [(StrKey::MountPolicy, "mp"), (StrKey::Vid, "V01001")]
        );
        assert_eq!(u64_options(&cmd), [(U64Key::MaxFilesToExpand, 10)]);
        assert_eq!(
            bool_options(&cmd),
            [(BoolKey::NoRecall, true), (BoolKey::Justmove, true)]
        );
    }

    #[tokio::test]
    async fn sends_the_commands() {
        let records = parse::<RepackLsItem>(cta_admin_jsonl!("repacks.jsonl"));
        let reply = Reply::records([Data::RelsItem(records[1].clone())]);
        let (request, cmd) = run(
            reply,
            |c| async move { c.repack_requests().get("I72052").await },
        )
        .await;
        assert_eq!(request.vid, "I72052");
        assert_eq!(request.state, RepackState::Running);
        assert_eq!(
            cmd,
            Sent::new(Cmd::Repack, SubCmd::SubcmdLs).str(StrKey::Vid, "I72052")
        );

        let cmd = listed(|c| async move { c.repack_requests().list().await }).await;
        assert_eq!(cmd, Sent::new(Cmd::Repack, SubCmd::SubcmdLs));

        let cmd = sent(|c| async move { c.repack_requests().delete("I72052").await }).await;
        assert_eq!(
            cmd,
            Sent::new(Cmd::Repack, SubCmd::SubcmdRm).str(StrKey::Vid, "I72052")
        );
    }
}
