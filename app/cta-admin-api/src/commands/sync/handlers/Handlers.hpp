/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/AdminCmdOptions.hpp"
#include "commands/sync/SyncCmdContext.hpp"

#include "cta_frontend.pb.h"

/// @file
/// The handlers of the non-streamed admin commands, one per (command, subcommand) pair, registered in
/// SyncCmdRegistry.cpp.
///
/// Each handler receives what it needs from the server (ctx), the options of the command (opts) and the response to
/// send back. The response is already set to RSP_SUCCESS when a handler is called: handlers only fill in a message
/// when they have one. Errors are reported by throwing, cta::exception::UserError for the errors of the user.
namespace cta::admin_api::commands::sync::handlers {

// handlers/Organization.cpp: admin users and virtual organizations

/// @brief cta-admin admin add
void addAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin admin ch
void changeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin admin rm
void removeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin virtualorganization add
void addVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin virtualorganization ch
void changeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin virtualorganization rm
void removeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/Storage.cpp: storage classes, tape pools and archive routes

/// @brief cta-admin archiveroute add
void addArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin archiveroute ch
void changeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin archiveroute rm
void removeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin storageclass add
void addStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin storageclass ch
void changeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin storageclass rm
void removeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tapepool add
void addTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tapepool ch
void changeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tapepool rm
void removeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/Libraries.cpp: logical and physical libraries, media types

/// @brief cta-admin logicallibrary add
void addLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin logicallibrary ch
void changeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin logicallibrary rm
void removeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mediatype add
void addMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mediatype ch
void changeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mediatype rm
void removeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin physicallibrary add
void addPhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin physicallibrary ch
void changePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin physicallibrary rm
void removePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/Disk.cpp: disk instances, disk instance spaces and disk systems

/// @brief cta-admin disksystem add
void addDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin disksystem rm
void removeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin disksystem ch
void changeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstance add
void addDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstance rm
void removeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstance ch
void changeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstancespace add
void addDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstancespace rm
void removeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin diskinstancespace ch
void changeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/MountRules.cpp: group, requester and activity mount rules, mount policies

/// @brief cta-admin groupmountrule add
void addGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin groupmountrule ch
void changeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin groupmountrule rm
void removeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mountpolicy add
void addMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mountpolicy ch
void changeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin mountpolicy rm
void removeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin requestermountrule add
void addRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin requestermountrule ch
void changeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin requestermountrule rm
void removeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin activitymountrule add
void addActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin activitymountrule ch
void changeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin activitymountrule rm
void removeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/Tapes.cpp: tapes, tape files, recycled tape files and archive files

/// @brief cta-admin tape add
void addTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tape ch
void changeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tape rm
void removeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tape reclaim
void reclaimTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tapefile ch
///
/// User errors are reported in the response (RSP_ERR_USER) instead of being thrown.
void changeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin tapefile rm
void removeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin recycletf restore
void restoreRecycleTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin archivefile ch
///
/// Kept for standalone tools still using CMD_ARCHIVEFILE, such as cta-eos-namespace-inject. Will be
/// removed later. User errors are reported in the response (RSP_ERR_USER) instead of being thrown.
void changeArchiveFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

// handlers/Scheduling.cpp: drives, repacks and failed requests

/// @brief cta-admin drive up
///
/// The outcome for each matching drive is returned in the message text.
void setDriveUp(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin drive down
///
/// The outcome for each matching drive is returned in the message text.
void setDriveDown(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin drive ch
///
/// The outcome for each matching drive is returned in the message text.
void changeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin drive rm
///
/// The outcome for each matching drive is returned in the message text.
void removeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin failedrequest rm
void removeFailedRequest(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin repack add
void addRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin repack rm
void removeRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

/// @brief cta-admin repack err
///
/// Not implemented: only returns a message saying so.
void showRepackErrors(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response);

}  // namespace cta::admin_api::commands::sync::handlers
