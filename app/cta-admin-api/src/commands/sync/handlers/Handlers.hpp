/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/AdminCmdOptions.hpp"
#include "commands/sync/SyncCmdContext.hpp"

#include <optional>
#include <string>

/// @file
/// The handlers of the non-streamed admin commands, one per (command, subcommand) pair, registered in
/// SyncCmdRegistry.cpp.
///
/// Each handler receives what it needs from the server (ctx) and the options of the command (opts), and returns the
/// message to send back to the client, if it has one. Errors are reported by throwing, cta::exception::UserError for
/// the errors of the user.
namespace cta::admin_api::commands::sync::handlers {

// handlers/Organization.cpp: admin users and virtual organizations

/// @brief cta-admin admin add
std::optional<std::string> addAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin admin ch
std::optional<std::string> changeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin admin rm
std::optional<std::string> removeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin virtualorganization add
std::optional<std::string> addVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin virtualorganization ch
std::optional<std::string> changeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin virtualorganization rm
std::optional<std::string> removeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/Storage.cpp: storage classes, tape pools and archive routes

/// @brief cta-admin archiveroute add
std::optional<std::string> addArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin archiveroute ch
std::optional<std::string> changeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin archiveroute rm
std::optional<std::string> removeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin storageclass add
std::optional<std::string> addStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin storageclass ch
std::optional<std::string> changeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin storageclass rm
std::optional<std::string> removeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tapepool add
std::optional<std::string> addTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tapepool ch
std::optional<std::string> changeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tapepool rm
std::optional<std::string> removeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/Libraries.cpp: logical and physical libraries, media types

/// @brief cta-admin logicallibrary add
std::optional<std::string> addLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin logicallibrary ch
std::optional<std::string> changeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin logicallibrary rm
std::optional<std::string> removeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mediatype add
std::optional<std::string> addMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mediatype ch
std::optional<std::string> changeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mediatype rm
std::optional<std::string> removeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin physicallibrary add
std::optional<std::string> addPhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin physicallibrary ch
std::optional<std::string> changePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin physicallibrary rm
std::optional<std::string> removePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/Disk.cpp: disk instances, disk instance spaces and disk systems

/// @brief cta-admin disksystem add
std::optional<std::string> addDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin disksystem rm
std::optional<std::string> removeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin disksystem ch
std::optional<std::string> changeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstance add
std::optional<std::string> addDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstance rm
std::optional<std::string> removeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstance ch
std::optional<std::string> changeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstancespace add
std::optional<std::string> addDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstancespace rm
std::optional<std::string> removeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin diskinstancespace ch
std::optional<std::string> changeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/MountRules.cpp: group, requester and activity mount rules, mount policies

/// @brief cta-admin groupmountrule add
std::optional<std::string> addGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin groupmountrule ch
std::optional<std::string> changeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin groupmountrule rm
std::optional<std::string> removeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mountpolicy add
std::optional<std::string> addMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mountpolicy ch
std::optional<std::string> changeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin mountpolicy rm
std::optional<std::string> removeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin requestermountrule add
std::optional<std::string> addRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin requestermountrule ch
std::optional<std::string> changeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin requestermountrule rm
std::optional<std::string> removeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin activitymountrule add
std::optional<std::string> addActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin activitymountrule ch
std::optional<std::string> changeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin activitymountrule rm
std::optional<std::string> removeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/Tapes.cpp: tapes, tape files, recycled tape files and archive files

/// @brief cta-admin tape add
std::optional<std::string> addTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tape ch
std::optional<std::string> changeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tape rm
std::optional<std::string> removeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tape reclaim
std::optional<std::string> reclaimTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tapefile ch
///
/// User errors are reported in the response (RSP_ERR_USER) instead of being thrown.
std::optional<std::string> changeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin tapefile rm
std::optional<std::string> removeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin recycletf restore
std::optional<std::string> restoreRecycleTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin archivefile ch
///
/// Kept for standalone tools still using CMD_ARCHIVEFILE, such as cta-eos-namespace-inject. Will be
/// removed later. User errors are reported in the response (RSP_ERR_USER) instead of being thrown.
std::optional<std::string> changeArchiveFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

// handlers/Scheduling.cpp: drives, repacks and failed requests

/// @brief cta-admin drive up
///
/// The outcome for each matching drive is returned in the message text.
std::optional<std::string> setDriveUp(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin drive down
///
/// The outcome for each matching drive is returned in the message text.
std::optional<std::string> setDriveDown(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin drive ch
///
/// The outcome for each matching drive is returned in the message text.
std::optional<std::string> changeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin drive rm
///
/// The outcome for each matching drive is returned in the message text.
std::optional<std::string> removeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin failedrequest rm
std::optional<std::string> removeFailedRequest(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin repack add
std::optional<std::string> addRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

/// @brief cta-admin repack rm
std::optional<std::string> removeRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts);

}  // namespace cta::admin_api::commands::sync::handlers
