// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use cta_protobuf::cta::admin::{
    AdminCmd, OptionBoolean, OptionStrList, OptionString, OptionUInt64,
    admin_cmd::{Cmd, SubCmd},
    option_boolean::Key as BoolKey,
    option_str_list::Key as StrListKey,
    option_string::Key as StrKey,
    option_u_int64::Key as U64Key,
};

use super::{
    StreamingAdminCmd, TypedAdminCmd, UnaryAdminCmd, activity_mount_rule, admin, archive_route,
    disk_instance, disk_instance_space, disk_system, drive, failed_request, group_mount_rule,
    logical_library, media_type, mount_policy, physical_library, recycle_tape_file, repack,
    requester_mount_rule, show_queues, storage_class, tape, tape_file, tape_pool, version,
    virtual_organization,
};
use crate::{
    admin::commands::{CLIENT_VERSION, PROTOBUF_TAG},
    typed_admin_cmd,
};

/// (command, subcommand, streaming) of a unary command
fn unary<T: UnaryAdminCmd>() -> (Cmd, SubCmd, bool) {
    (T::CMD, T::SUBCMD, false)
}

/// (command, subcommand, streaming) of a streaming command
fn streaming<T: StreamingAdminCmd>() -> (Cmd, SubCmd, bool) {
    (T::CMD, T::SUBCMD, true)
}

/// Every typed command, except `failed_request::Summary` which shares the pair of `failed_request::Ls`
fn typed_pairs() -> Vec<(Cmd, SubCmd, bool)> {
    vec![
        unary::<admin::Add>(),
        unary::<admin::Ch>(),
        unary::<admin::Rm>(),
        streaming::<admin::Ls>(),
        unary::<virtual_organization::Add>(),
        unary::<virtual_organization::Ch>(),
        unary::<virtual_organization::Rm>(),
        streaming::<virtual_organization::Ls>(),
        unary::<disk_instance::Add>(),
        unary::<disk_instance::Ch>(),
        unary::<disk_instance::Rm>(),
        streaming::<disk_instance::Ls>(),
        unary::<disk_instance_space::Add>(),
        unary::<disk_instance_space::Ch>(),
        unary::<disk_instance_space::Rm>(),
        streaming::<disk_instance_space::Ls>(),
        unary::<disk_system::Add>(),
        unary::<disk_system::Ch>(),
        unary::<disk_system::Rm>(),
        streaming::<disk_system::Ls>(),
        streaming::<recycle_tape_file::Ls>(),
        unary::<recycle_tape_file::Restore>(),
        unary::<tape_file::Ch>(),
        unary::<tape_file::Rm>(),
        streaming::<tape_file::Ls>(),
        unary::<drive::Up>(),
        unary::<drive::Down>(),
        streaming::<drive::Ls>(),
        unary::<drive::Rm>(),
        unary::<drive::Ch>(),
        unary::<logical_library::Add>(),
        unary::<logical_library::Ch>(),
        unary::<logical_library::Rm>(),
        streaming::<logical_library::Ls>(),
        unary::<media_type::Add>(),
        unary::<media_type::Ch>(),
        unary::<media_type::Rm>(),
        streaming::<media_type::Ls>(),
        unary::<physical_library::Add>(),
        unary::<physical_library::Ch>(),
        unary::<physical_library::Rm>(),
        streaming::<physical_library::Ls>(),
        unary::<activity_mount_rule::Add>(),
        unary::<activity_mount_rule::Ch>(),
        unary::<activity_mount_rule::Rm>(),
        streaming::<activity_mount_rule::Ls>(),
        unary::<group_mount_rule::Add>(),
        unary::<group_mount_rule::Ch>(),
        unary::<group_mount_rule::Rm>(),
        streaming::<group_mount_rule::Ls>(),
        unary::<mount_policy::Add>(),
        unary::<mount_policy::Ch>(),
        unary::<mount_policy::Rm>(),
        streaming::<mount_policy::Ls>(),
        unary::<requester_mount_rule::Add>(),
        unary::<requester_mount_rule::Ch>(),
        unary::<requester_mount_rule::Rm>(),
        streaming::<requester_mount_rule::Ls>(),
        streaming::<failed_request::Ls>(),
        unary::<failed_request::Rm>(),
        streaming::<show_queues::ShowQueues>(),
        unary::<repack::Add>(),
        unary::<repack::Rm>(),
        streaming::<repack::Ls>(),
        unary::<repack::Err>(),
        unary::<archive_route::Add>(),
        unary::<archive_route::Ch>(),
        unary::<archive_route::Rm>(),
        streaming::<archive_route::Ls>(),
        unary::<storage_class::Add>(),
        unary::<storage_class::Ch>(),
        unary::<storage_class::Rm>(),
        streaming::<storage_class::Ls>(),
        unary::<tape_pool::Add>(),
        unary::<tape_pool::Ch>(),
        unary::<tape_pool::Rm>(),
        streaming::<tape_pool::Ls>(),
        unary::<tape::Add>(),
        unary::<tape::Ch>(),
        unary::<tape::Rm>(),
        unary::<tape::Reclaim>(),
        streaming::<tape::Ls>(),
        streaming::<version::Version>(),
    ]
}

fn string(key: StrKey, value: &str) -> OptionString {
    OptionString {
        key: key.into(),
        value: value.to_owned(),
    }
}

fn uint(key: U64Key, value: u64) -> OptionUInt64 {
    OptionUInt64 {
        key: key.into(),
        value,
    }
}

fn boolean(key: BoolKey, value: bool) -> OptionBoolean {
    OptionBoolean {
        key: key.into(),
        value,
    }
}

#[test]
fn commands_are_unique() {
    let pairs = typed_pairs();
    // 82 (command, subcommand) pairs from CLI plus `recycletf restore`
    assert_eq!(pairs.len(), 83);
    for (i, (cmd, subcmd, _)) in pairs.iter().enumerate() {
        assert!(
            !pairs[..i].iter().any(|(c, s, _)| (c, s) == (cmd, subcmd)),
            "{cmd:?} {subcmd:?} is typed twice"
        );
    }
}

#[test]
fn streaming_commands() {
    // `streamCmds` in `tools/CtaAdminCmdParser.hpp`: every `ls`, plus `showqueues` and `version`
    let streaming_count = typed_pairs().into_iter().filter(|(_, _, s)| *s).count();
    assert_eq!(streaming_count, 23);
    // not in `typed_pairs`, as it shares the pair of `failed_request::Ls`
    streaming::<failed_request::Summary>();
}

/// The record of each streaming command is the one of its `#[item]`, any other record is given back
#[test]
fn streaming_items() {
    use cta_protobuf::cta::{
        admin::{FailedRequestLsItem, FailedRequestLsSummary, TapeLsItem, VersionItem},
        xrd::data::Data,
    };

    let tape = TapeLsItem {
        vid: "V01001".into(),
        ..Default::default()
    };
    assert_eq!(
        tape::Ls::item(Data::TalsItem(tape.clone())).unwrap().vid,
        "V01001"
    );
    assert!(matches!(
        tape::Ls::item(Data::VersionItem(VersionItem::default())),
        Err(other) if matches!(*other, Data::VersionItem(_))
    ));
    assert!(version::Version::item(Data::VersionItem(VersionItem::default())).is_ok());
    // `failedrequest ls` and `failedrequest ls --summary` share a pair, not a record type
    assert!(failed_request::Ls::item(Data::FrlsItem(FailedRequestLsItem::default())).is_ok());
    assert!(
        failed_request::Summary::item(Data::FrlsSummary(FailedRequestLsSummary::default())).is_ok()
    );
    assert!(failed_request::Summary::item(Data::FrlsItem(FailedRequestLsItem::default())).is_err());
}

#[test]
fn conversion_matches_constants() {
    fn check<T: TypedAdminCmd + Default>() {
        let cmd: AdminCmd = T::default().into();
        assert_eq!((cmd.cmd(), cmd.subcmd()), (T::CMD, T::SUBCMD));
    }
    check::<tape::Ls>();
    check::<version::Version>();
    check::<show_queues::ShowQueues>();
    check::<failed_request::Summary>();
    let cmd: AdminCmd = drive::Up::builder().name("dr").build().into();
    assert_eq!(
        (cmd.cmd(), cmd.subcmd()),
        (drive::Up::CMD, drive::Up::SUBCMD)
    );
}

#[test]
fn sets_versions() {
    let cmd: AdminCmd = version::Version::default().into();
    assert_eq!(cmd.cmd(), Cmd::Version);
    assert_eq!(cmd.subcmd(), SubCmd::SubcmdNone);
    assert_eq!(cmd.client_version, CLIENT_VERSION);
    assert_eq!(cmd.protobuf_tag, PROTOBUF_TAG);
    assert!(!cmd.client_version.is_empty());
    assert!(!cmd.protobuf_tag.is_empty());
}

#[test]
fn unset_options_are_not_sent() {
    let cmd: AdminCmd = tape::Ls::default().into();
    assert!(cmd.option_str.is_empty());
    assert!(cmd.option_uint64.is_empty());
    assert!(cmd.option_bool.is_empty());
    assert!(cmd.option_str_list.is_empty());
}

#[test]
fn every_option_type() {
    let cmd: AdminCmd = tape::Ls::builder()
        .vid("V1")
        .capacity(10)
        .full(false)
        .disk_file_ids(vec!["1".to_owned(), "2".to_owned()])
        .all(true)
        .missing_file_copies(false)
        .build()
        .into();
    assert_eq!(cmd.option_str, [string(StrKey::Vid, "V1")]);
    assert_eq!(cmd.option_uint64, [uint(U64Key::Capacity, 10)]);
    // `false` flags are not sent, `false` booleans are
    assert_eq!(
        cmd.option_bool,
        [boolean(BoolKey::Full, false), boolean(BoolKey::All, true)]
    );
    assert_eq!(
        cmd.option_str_list,
        [OptionStrList {
            key: StrListKey::FileId.into(),
            item: vec!["1".to_owned(), "2".to_owned()],
        }]
    );
}

#[test]
fn required_and_optional_options() {
    let cmd: AdminCmd = tape_pool::Add::builder()
        .name("tp")
        .vo("vo")
        .partial_tapes(2)
        .supply("a,b")
        .comment("c")
        .build()
        .into();
    assert_eq!(
        cmd.option_str,
        [
            string(StrKey::TapePool, "tp"),
            string(StrKey::Vo, "vo"),
            string(StrKey::Supply, "a,b"),
            string(StrKey::Comment, "c"),
        ]
    );
    assert_eq!(cmd.option_uint64, [uint(U64Key::PartialTapesNumber, 2)]);
}

#[test]
fn converted_options() {
    use cta_model::{routing::ArchiveRouteType, tape::TapeState};

    let cmd: AdminCmd = tape::Ls::builder()
        .state(TapeState::RepackingDisabled)
        .build()
        .into();
    assert_eq!(
        cmd.option_str,
        vec![string(StrKey::State, "REPACKING_DISABLED")]
    );

    let cmd: AdminCmd = archive_route::Rm::builder()
        .storage_class("sc")
        .copy_nb(1)
        .archive_route_type(ArchiveRouteType::Repack)
        .build()
        .into();
    assert!(
        cmd.option_str
            .contains(&string(StrKey::ArchiveRouteType, "REPACK"))
    );

    let cmd = crate::admin_cmd!(Tape.SubcmdLs {
        State: Option<TapeState> => Some(TapeState::Active),
        Vid: Option<String> => None,
        Capacity: u64 => 5u32,
        Full: Option<bool> => Some(true),
        FileId: Vec<String> => ["1", "2"].map(String::from).to_vec(),
    });
    assert_eq!(cmd.option_str, vec![string(StrKey::State, "ACTIVE")]);
    assert_eq!(cmd.option_uint64, vec![uint(U64Key::Capacity, 5)]);
    assert_eq!(cmd.option_bool, vec![boolean(BoolKey::Full, true)]);
    assert_eq!(
        cmd.option_str_list,
        vec![OptionStrList {
            key: StrListKey::FileId.into(),
            item: vec!["1".to_owned(), "2".to_owned()],
        }]
    );
}

/// Domain values of the test of non-string targets
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Capacity(u64);

impl super::OptionValue for Capacity {
    type Key = U64Key;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        self.0.push(cmd, key);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fullness {
    Full,
    NotFull,
}

impl super::OptionValue for Fullness {
    type Key = BoolKey;

    fn push(self, cmd: &mut AdminCmd, key: Self::Key) {
        (self == Fullness::Full).push(cmd, key);
    }
}

/// A tape listing with options of other types than strings
#[typed_admin_cmd(Tape::Ls)]
struct ConvertedLs {
    pub capacity: Capacity,
    pub full: Option<Fullness>,
}

#[test]
fn converted_options_of_other_types() {
    let cmd: AdminCmd = ConvertedLs::builder()
        .capacity(Capacity(42))
        .full(Fullness::NotFull)
        .build()
        .into();

    assert!(cmd.option_str.is_empty());
    assert_eq!(cmd.option_uint64, vec![uint(U64Key::Capacity, 42)]);
    assert_eq!(cmd.option_bool, vec![boolean(BoolKey::Full, false)]);
}

#[test]
fn failed_request_summary() {
    let cmd: AdminCmd = failed_request::Summary::builder()
        .just_archive(true)
        .build()
        .into();
    assert_eq!(
        cmd.option_bool,
        [
            boolean(BoolKey::Justarchive, true),
            boolean(BoolKey::Summary, true)
        ]
    );
    assert!(cmd.option_str.is_empty());
}

/// The `(command, subcommand)` pairs of a table of the admin API (`app/cta-admin-api`), read from its
/// C++ source: the `{AdminCmd::CMD_..., AdminCmd::SUBCMD_...}` keys between `<table> = {` and the closing `};`.
///
/// This is a bit of a fragile test, but it gives us an important validation that the refactored admin API (`cta-client`)
/// is consistent with the C++ implementation of the admin API (`app/cta-admin-api`). In a future Rust implementation of
/// the admin API, we could use the same source to generate both server and client code.
fn server_table(source: &str, table: &str) -> Vec<(Cmd, SubCmd)> {
    let start = source
        .find(&format!("{table} = {{"))
        .unwrap_or_else(|| panic!("{table} not found"));
    let body = &source[start..];
    let body = &body[..body.find("\n};").expect("end of the table")];
    body.split("{AdminCmd::")
        .skip(1)
        .map(|entry| {
            let (cmd, rest) = entry.split_once(',').expect("a (cmd, subcmd) pair");
            let subcmd = rest
                .trim_start()
                .strip_prefix("AdminCmd::")
                .and_then(|s| s.split('}').next())
                .expect("a subcommand");
            (
                Cmd::from_str_name(cmd.trim()).unwrap_or_else(|| panic!("unknown command {cmd}")),
                SubCmd::from_str_name(subcmd.trim())
                    .unwrap_or_else(|| panic!("unknown subcommand {subcmd}")),
            )
        })
        .collect()
}

/// Check that the kind of each command (unary or streaming) routes it to the service implementing it on the admin API: streaming commands
/// to `STREAM_CMD_TABLE` (`GenericAdminStream`), the others to `SYNC_CMD_TABLE` (`Admin`).
#[test]
fn streaming_matches_admin_api() {
    let stream_table = server_table(
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../app/cta-admin-api/src/commands/stream/StreamFactory.cpp"
        )),
        "STREAM_CMD_TABLE",
    );
    let sync_table = server_table(
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../app/cta-admin-api/src/commands/sync/SyncCmdRegistry.cpp"
        )),
        "SYNC_CMD_TABLE",
    );
    assert_eq!(stream_table.len(), 23);

    let typed = typed_pairs();
    let mut unimplemented = Vec::new();
    for (cmd, subcmd, streaming) in &typed {
        let (table, other) = if *streaming {
            (&stream_table, &sync_table)
        } else {
            (&sync_table, &stream_table)
        };
        assert!(
            !other.contains(&(*cmd, *subcmd)),
            "{cmd:?} {subcmd:?} (streaming: {streaming}) is in the wrong table of the admin API"
        );
        if !table.contains(&(*cmd, *subcmd)) {
            unimplemented.push((*cmd, *subcmd));
        }
    }
    // `cta-admin repack err` exists, but the frontend never implemented it
    assert_eq!(unimplemented, [(Cmd::Repack, SubCmd::SubcmdErr)]);

    // every streaming command of the admin API has a typed command
    for (cmd, subcmd) in &stream_table {
        assert!(
            typed.iter().any(|(c, s, _)| (c, s) == (cmd, subcmd)),
            "{cmd:?} {subcmd:?} has no typed command"
        );
    }
    // the unary commands of the admin API without a typed command: `archivefile ch`, kept for
    // `cta-eos-namespace-inject` and not available in `cta-admin`
    let untyped: Vec<_> = sync_table
        .iter()
        .filter(|(cmd, subcmd)| !typed.iter().any(|(c, s, _)| (c, s) == (cmd, subcmd)))
        .collect();
    assert_eq!(untyped, [&(Cmd::Archivefile, SubCmd::SubcmdCh)]);
}
