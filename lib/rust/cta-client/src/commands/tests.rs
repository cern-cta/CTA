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
    TypedAdminCmd, activity_mount_rule, admin, archive_route, disk_instance, disk_instance_space,
    disk_system, drive, failed_request, group_mount_rule, logical_library, media_type,
    mount_policy, physical_library, recycle_tape_file, repack, requester_mount_rule, show_queues,
    storage_class, tape, tape_file, tape_pool, version, virtual_organization,
};
use crate::commands::{CLIENT_VERSION, PROTOBUF_TAG};

/// (command, subcommand, streaming) of a typed command
fn pair<T: TypedAdminCmd>() -> (Cmd, SubCmd, bool) {
    (T::CMD, T::SUBCMD, T::STREAMING)
}

/// Every typed command, except `failed_request::Summary` which shares the pair of `failed_request::Ls`
fn typed_pairs() -> Vec<(Cmd, SubCmd, bool)> {
    vec![
        pair::<admin::Add>(),
        pair::<admin::Ch>(),
        pair::<admin::Rm>(),
        pair::<admin::Ls>(),
        pair::<virtual_organization::Add>(),
        pair::<virtual_organization::Ch>(),
        pair::<virtual_organization::Rm>(),
        pair::<virtual_organization::Ls>(),
        pair::<disk_instance::Add>(),
        pair::<disk_instance::Ch>(),
        pair::<disk_instance::Rm>(),
        pair::<disk_instance::Ls>(),
        pair::<disk_instance_space::Add>(),
        pair::<disk_instance_space::Ch>(),
        pair::<disk_instance_space::Rm>(),
        pair::<disk_instance_space::Ls>(),
        pair::<disk_system::Add>(),
        pair::<disk_system::Ch>(),
        pair::<disk_system::Rm>(),
        pair::<disk_system::Ls>(),
        pair::<recycle_tape_file::Ls>(),
        pair::<tape_file::Ch>(),
        pair::<tape_file::Rm>(),
        pair::<tape_file::Ls>(),
        pair::<drive::Up>(),
        pair::<drive::Down>(),
        pair::<drive::Ls>(),
        pair::<drive::Rm>(),
        pair::<drive::Ch>(),
        pair::<logical_library::Add>(),
        pair::<logical_library::Ch>(),
        pair::<logical_library::Rm>(),
        pair::<logical_library::Ls>(),
        pair::<media_type::Add>(),
        pair::<media_type::Ch>(),
        pair::<media_type::Rm>(),
        pair::<media_type::Ls>(),
        pair::<physical_library::Add>(),
        pair::<physical_library::Ch>(),
        pair::<physical_library::Rm>(),
        pair::<physical_library::Ls>(),
        pair::<activity_mount_rule::Add>(),
        pair::<activity_mount_rule::Ch>(),
        pair::<activity_mount_rule::Rm>(),
        pair::<activity_mount_rule::Ls>(),
        pair::<group_mount_rule::Add>(),
        pair::<group_mount_rule::Ch>(),
        pair::<group_mount_rule::Rm>(),
        pair::<group_mount_rule::Ls>(),
        pair::<mount_policy::Add>(),
        pair::<mount_policy::Ch>(),
        pair::<mount_policy::Rm>(),
        pair::<mount_policy::Ls>(),
        pair::<requester_mount_rule::Add>(),
        pair::<requester_mount_rule::Ch>(),
        pair::<requester_mount_rule::Rm>(),
        pair::<requester_mount_rule::Ls>(),
        pair::<failed_request::Ls>(),
        pair::<failed_request::Rm>(),
        pair::<show_queues::ShowQueues>(),
        pair::<repack::Add>(),
        pair::<repack::Rm>(),
        pair::<repack::Ls>(),
        pair::<repack::Err>(),
        pair::<archive_route::Add>(),
        pair::<archive_route::Ch>(),
        pair::<archive_route::Rm>(),
        pair::<archive_route::Ls>(),
        pair::<storage_class::Add>(),
        pair::<storage_class::Ch>(),
        pair::<storage_class::Rm>(),
        pair::<storage_class::Ls>(),
        pair::<tape_pool::Add>(),
        pair::<tape_pool::Ch>(),
        pair::<tape_pool::Rm>(),
        pair::<tape_pool::Ls>(),
        pair::<tape::Add>(),
        pair::<tape::Ch>(),
        pair::<tape::Rm>(),
        pair::<tape::Reclaim>(),
        pair::<tape::Ls>(),
        pair::<version::Version>(),
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
    // the (command, subcommand) pairs of `cta-admin`, see `cmdOptions` in `tools/CtaAdminCmdParser.hpp`
    assert_eq!(pairs.len(), 82);
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
    let streaming = typed_pairs().into_iter().filter(|(_, _, s)| *s).count();
    assert_eq!(streaming, 23);
    const { assert!(show_queues::ShowQueues::STREAMING) };
    const { assert!(version::Version::STREAMING) };
    const { assert!(failed_request::Summary::STREAMING) };
    const { assert!(tape::Ls::STREAMING) };
    const { assert!(!tape::Reclaim::STREAMING) };
    const { assert!(!drive::Up::STREAMING) };
    const { assert!(!repack::Err::STREAMING) };
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
