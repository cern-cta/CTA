// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The helpers of the DAO tests, and the tests of the behaviour shared by the DAOs (`get`, errors, files of IDs).
//!
//! The tests of each group (`<group>/tests.rs`) run the data access objects against the mock frontend: the commands
//! they send, and what they make of the records.

use cta_protobuf::cta::{
    admin::{
        AdminCmd, AdminLsItem, OptionBoolean, OptionStrList, OptionString, OptionUInt64,
        TapeLsItem, TapePoolLsItem, Version, VersionItem,
        admin_cmd::{Cmd, SubCmd},
        option_boolean::Key as BoolKey,
        option_str_list::Key as StrListKey,
        option_string::Key as StrKey,
        option_u_int64::Key as U64Key,
    },
    xrd::data::Data,
};

use super::id_file::parse_disk_file_ids;
use crate::admin::{
    AdminClient, AdminError,
    dao::IdFileError,
    test_support::{Reply, cta_admin_jsonl, parse, serve, single_call},
};

/// A command as the frontend received it, with its options sorted by key: the expectations do not depend on the order
/// of the fields of the typed commands.
#[derive(Debug, PartialEq)]
pub(super) struct Sent {
    cmd: Cmd,
    subcmd: SubCmd,
    strs: Vec<(StrKey, String)>,
    u64s: Vec<(U64Key, u64)>,
    bools: Vec<(BoolKey, bool)>,
    str_lists: Vec<(StrListKey, Vec<String>)>,
}

impl Sent {
    /// `cmd subcmd`, without options.
    pub(super) fn new(cmd: Cmd, subcmd: SubCmd) -> Self {
        Self {
            cmd,
            subcmd,
            strs: Vec::new(),
            u64s: Vec::new(),
            bools: Vec::new(),
            str_lists: Vec::new(),
        }
    }

    /// With a string option.
    pub(super) fn str(mut self, key: StrKey, value: &str) -> Self {
        self.strs.push((key, value.to_owned()));
        self.strs.sort();
        self
    }

    /// With an integer option.
    pub(super) fn u64(mut self, key: U64Key, value: u64) -> Self {
        self.u64s.push((key, value));
        self.u64s.sort();
        self
    }

    /// With a boolean option, or a flag.
    pub(super) fn bool(mut self, key: BoolKey, value: bool) -> Self {
        self.bools.push((key, value));
        self.bools.sort();
        self
    }

    /// With a string list option.
    pub(super) fn str_list(mut self, key: StrListKey, values: &[&str]) -> Self {
        self.str_lists
            .push((key, values.iter().map(|&v| v.to_owned()).collect()));
        self.str_lists.sort();
        self
    }
}

impl From<&AdminCmd> for Sent {
    fn from(cmd: &AdminCmd) -> Self {
        let sent = Self::new(cmd.cmd(), cmd.subcmd());
        let sent = cmd
            .option_str
            .iter()
            .fold(sent, |sent, o| sent.str(o.key(), &o.value));
        let sent = cmd
            .option_uint64
            .iter()
            .fold(sent, |sent, o| sent.u64(o.key(), o.value));
        let sent = cmd
            .option_bool
            .iter()
            .fold(sent, |sent, o| sent.bool(o.key(), o.value));
        cmd.option_str_list.iter().fold(sent, |sent, o| {
            let items: Vec<_> = o.item.iter().map(String::as_str).collect();
            sent.str_list(o.key(), &items)
        })
    }
}

/// Runs `call` against a mock frontend answering `reply`: its result, and the command it sent.
pub(super) async fn run<T, F>(reply: Reply, call: impl FnOnce(AdminClient) -> F) -> (T, Sent)
where
    F: Future<Output = Result<T, AdminError>>,
{
    let (client, calls) = serve(reply).await;
    let result = call(client).await.expect("the call succeeds");
    (result, Sent::from(&single_call(&calls).cmd))
}

/// The command sent by `call`, answered by a success (for the commands which change something).
pub(super) async fn sent<T, F>(call: impl FnOnce(AdminClient) -> F) -> Sent
where
    F: Future<Output = Result<T, AdminError>>,
{
    run(Reply::success(), call).await.1
}

/// The command sent by `call`, answered by an empty stream (for the commands listing something).
pub(super) async fn listed<T, F>(call: impl FnOnce(AdminClient) -> F) -> Sent
where
    F: Future<Output = Result<T, AdminError>>,
{
    run(Reply::records([]), call).await.1
}

/// The error of `call` against a mock frontend answering `reply`.
pub(super) async fn failure<T, F>(reply: Reply, call: impl FnOnce(AdminClient) -> F) -> AdminError
where
    F: Future<Output = Result<T, AdminError>>,
{
    let (client, _) = serve(reply).await;
    match call(client).await {
        Ok(_) => panic!("the call should fail"),
        Err(error) => error,
    }
}

/// The string options of a command, as `(key, value)`
pub(super) fn str_options(cmd: &AdminCmd) -> Vec<(StrKey, &str)> {
    cmd.option_str
        .iter()
        .map(|OptionString { value, .. }| (cmd_str_key(value, cmd), value.as_str()))
        .collect()
}

pub(super) fn cmd_str_key(value: &str, cmd: &AdminCmd) -> StrKey {
    cmd.option_str
        .iter()
        .find(|o| o.value == value)
        .map(OptionString::key)
        .unwrap()
}

pub(super) fn u64_options(cmd: &AdminCmd) -> Vec<(U64Key, u64)> {
    cmd.option_uint64
        .iter()
        .map(|o| (o.key(), o.value))
        .collect()
}

pub(super) fn bool_options(cmd: &AdminCmd) -> Vec<(BoolKey, bool)> {
    cmd.option_bool.iter().map(|o| (o.key(), o.value)).collect()
}

pub(super) fn str_list_options(cmd: &AdminCmd) -> Vec<(StrListKey, Vec<String>)> {
    cmd.option_str_list
        .iter()
        .map(|o| (o.key(), o.item.clone()))
        .collect()
}

pub(super) fn pair(cmd: &AdminCmd) -> (Cmd, SubCmd) {
    (cmd.cmd(), cmd.subcmd())
}

mod get {
    use super::*;

    #[tokio::test]
    async fn one_record_is_the_resource() {
        let records = parse::<TapePoolLsItem>(cta_admin_jsonl!("tape_pools.jsonl"));
        let (client, calls) = serve(Reply::records([Data::TplsItem(records[0].clone())])).await;

        let pool = client.tape_pools().get("atlaspps").await.unwrap();

        assert_eq!(pool.name, "atlaspps");
        assert_eq!(pool.statistics.file_count, 864);
        let call = single_call(&calls);
        assert_eq!(call.rpc, "GenericAdminStream");
        assert_eq!(pair(&call.cmd), (Cmd::Tapepool, SubCmd::SubcmdLs));
        assert_eq!(str_options(&call.cmd), [(StrKey::TapePool, "atlaspps")]);
    }

    #[tokio::test]
    async fn no_record_is_not_found() {
        let (client, _) = serve(Reply::records([])).await;

        let error = client.tape_pools().get("nope").await.unwrap_err();

        assert!(
            matches!(error, AdminError::NotFound { resource: "tape pool", ref id } if id == "nope"),
            "{error}"
        );
        assert_eq!(error.to_string(), "tape pool nope not found");
        assert!(error.status().is_none());
    }

    #[tokio::test]
    async fn several_records_are_a_conflict() {
        let records = parse::<TapePoolLsItem>(cta_admin_jsonl!("tape_pools.jsonl"));
        let (client, _) = serve(Reply::records(records.into_iter().map(Data::TplsItem))).await;

        let error = client.tape_pools().get("atlaspps").await.unwrap_err();

        assert!(
            matches!(
                error,
                AdminError::MoreThanOne {
                    resource: "tape pool",
                    count: 3,
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn filters_the_resources_without_ls_filter() {
        let records = parse::<AdminLsItem>(cta_admin_jsonl!("admin_users.jsonl"));
        let (client, calls) = serve(Reply::records(records.into_iter().map(Data::AdlsItem))).await;

        let user = client.admin_users().get("ccaffy").await.unwrap();
        assert_eq!(user.metadata.comment, "Cedric Caffy");

        let error = client.admin_users().get("nobody").await.unwrap_err();
        assert!(matches!(
            error,
            AdminError::NotFound {
                resource: "admin user",
                ..
            }
        ));

        // `admin ls` takes no option
        for call in calls.lock().unwrap().iter() {
            assert_eq!(pair(&call.cmd), (Cmd::Admin, SubCmd::SubcmdLs));
            assert!(call.cmd.option_str.is_empty());
        }
    }

    #[tokio::test]
    async fn user_errors_are_passed_through() {
        let (client, _) = serve(Reply::StreamFailure(tonic::Status::invalid_argument(
            "Tape pool nope does not exist",
        )))
        .await;

        let error = client.tape_pools().get("nope").await.unwrap_err();

        assert!(matches!(error, AdminError::User(_)), "{error}");
    }

    #[tokio::test]
    async fn unconvertible_records_are_invalid_responses() {
        let mut record = parse::<TapeLsItem>(cta_admin_jsonl!("tapes.jsonl")).remove(0);
        record.state = "LOST".into();
        let (client, _) = serve(Reply::records([Data::TalsItem(record)])).await;

        let error = client.tapes().get("I71007").await.unwrap_err();

        assert!(
            matches!(error, AdminError::InvalidResponse { ref message, source: Some(_) } if message.contains("LOST")),
            "{error}"
        );
    }
}

mod commands {
    use super::*;

    #[tokio::test]
    async fn api_errors_are_passed_through() {
        let (client, _) = serve(Reply::Unary(Err(tonic::Status::failed_precondition(
            "catalogue down",
        ))))
        .await;

        let error = client.admin_users().delete("afonso").await.unwrap_err();

        assert!(matches!(error, AdminError::API(_)), "{error}");
        assert_eq!(error.message(), "catalogue down");
    }
}

#[tokio::test]
async fn version() {
    let item = VersionItem {
        client_version: Some(Version {
            cta_version: "5.11.12.1-1".into(),
            ..Default::default()
        }),
        server_version: Some(Version {
            cta_version: "5.11.13.0-1".into(),
            xrootd_ssi_protobuf_interface_version: "v1".into(),
        }),
        catalogue_version: "15.0".into(),
        ..Default::default()
    };
    let (client, calls) = serve(Reply::records([Data::VersionItem(item)])).await;

    let version = client.version().await.unwrap();

    assert_eq!(version.client_version.version, "5.11.12.1-1");
    assert_eq!(version.client_version.protobuf_interface_version, None);
    assert_eq!(
        version.server_version.protobuf_interface_version.as_deref(),
        Some("v1")
    );
    assert_eq!(version.catalogue_version, "15.0");
    let cmd = single_call(&calls).cmd;
    assert_eq!(pair(&cmd), (Cmd::Version, SubCmd::SubcmdNone));
}

mod id_file {
    use super::*;

    #[test]
    fn keeps_the_fid_tokens_as_decimal() {
        let ids = parse_disk_file_ids(
            "path=/eos/a fid=0x1a\npath=/eos/b fid=00ff\n\n  # only a comment\nfid=10 # trailing comment fid=20\n",
        )
        .unwrap();

        assert_eq!(ids, ["26", "255", "16"]);
    }

    /// The example of the documentation of `read_disk_file_ids`
    #[test]
    fn documented_example() {
        let ids = parse_disk_file_ids(
            "# files to restore\n/data/dir/\n/data/dir/file.txt fid=8\n/data/dir/other.txt fid=0x1f\nfid=2a\n",
        )
        .unwrap();

        assert_eq!(ids, ["8", "31", "42"]);
    }

    #[test]
    fn ignores_other_tokens() {
        assert!(parse_disk_file_ids("1 2 3\nabc\n").unwrap().is_empty());
    }

    #[test]
    fn rejects_invalid_hex() {
        let error = parse_disk_file_ids("fid=xyz").unwrap_err();

        assert!(matches!(error, IdFileError::InvalidHex(ref fid) if fid == "xyz"));
        assert!(matches!(
            parse_disk_file_ids("fid=").unwrap_err(),
            IdFileError::InvalidHex(_)
        ));
    }

    #[test]
    fn reads_a_file() {
        let dir = tempdir();
        let path = dir.join("ids");
        std::fs::write(&path, "fid=a\n").unwrap();

        assert_eq!(
            crate::admin::dao::read_disk_file_ids(&path).unwrap(),
            ["10"]
        );
        assert!(matches!(
            crate::admin::dao::read_disk_file_ids(dir.join("missing")).unwrap_err(),
            IdFileError::Io(_)
        ));
    }
}

/// A fresh temporary directory for the test, removed when the process exits.
pub(super) fn tempdir() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "cta-client-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn option_helpers_read_the_keys() {
    // sanity check of the helpers of this module on a hand-made command
    let cmd = AdminCmd {
        option_str: vec![OptionString {
            key: StrKey::Vid.into(),
            value: "V".into(),
        }],
        option_uint64: vec![OptionUInt64 {
            key: U64Key::Capacity.into(),
            value: 1,
        }],
        option_bool: vec![OptionBoolean {
            key: BoolKey::All.into(),
            value: true,
        }],
        option_str_list: vec![OptionStrList {
            key: StrListKey::Vid.into(),
            item: vec!["a".into()],
        }],
        ..Default::default()
    };
    assert_eq!(str_options(&cmd), [(StrKey::Vid, "V")]);
    assert_eq!(u64_options(&cmd), [(U64Key::Capacity, 1)]);
    assert_eq!(bool_options(&cmd), [(BoolKey::All, true)]);
    assert_eq!(
        str_list_options(&cmd),
        [(StrListKey::Vid, vec!["a".to_owned()])]
    );
}
