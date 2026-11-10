// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk system commands

use crate::define_typed_admin_cmd;

define_typed_admin_cmd! {
    /// Adds a disk system (`disksystem add`).
    #[proto(Disksystem.SubcmdAdd)]
    pub struct Add {
        /// Name of the disk system
        disk_system: str => DiskSystem,
        /// Regular expression matching the destination URLs of the disk system
        file_regexp: str => FileRegexp,
        /// Disk instance of the disk system
        disk_instance: str => DiskInstance,
        /// Disk instance space of the disk system
        disk_instance_space: str => DiskInstanceSpace,
        /// Free space to keep before processing requests, in bytes
        targeted_free_space: u64 => TargetedFreeSpace,
        /// How long to wait when the free space is insufficient, in seconds
        sleep_time: u64 => SleepTime,
        /// Comment
        comment: str => Comment,
    }
}

define_typed_admin_cmd! {
    /// Changes a disk system (`disksystem ch`).
    #[proto(Disksystem.SubcmdCh)]
    pub struct Ch {
        /// Name of the disk system
        disk_system: str => DiskSystem,
        /// New regular expression matching the destination URLs
        file_regexp: str? => FileRegexp,
        /// New free space to keep, in bytes
        targeted_free_space: u64? => TargetedFreeSpace,
        /// New waiting time, in seconds
        sleep_time: u64? => SleepTime,
        /// New comment
        comment: str? => Comment,
    }
}

define_typed_admin_cmd! {
    /// Removes a disk system (`disksystem rm`).
    #[proto(Disksystem.SubcmdRm)]
    pub struct Rm {
        /// Name of the disk system
        disk_system: str => DiskSystem,
    }
}

define_typed_admin_cmd! {
    /// Lists the disk systems (`disksystem ls`).
    #[proto(Disksystem.SubcmdLs)]
    #[item(DslsItem: DiskSystemLsItem)]
    #[derive(Default)]
    pub struct Ls {}
}
