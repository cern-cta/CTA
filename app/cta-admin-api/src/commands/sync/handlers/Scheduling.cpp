/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/handlers/Handlers.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"

#include <algorithm>
#include <sstream>
#include <vector>

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

namespace {

/// Changes the desired state of the drives whose name matches a regex
///
/// @param ctx                What the handlers need from the server
/// @param regex              The regex to match the drive name(s) to change
/// @param desiredDriveState  Desired drive state (up, forceDown, reason, comment)
///
/// @return The outcome for each matching drive, to return to the client
std::string setDriveState(const SyncCmdContext& ctx,
                          const std::string& regex,
                          const common::dataStructures::DesiredDriveState& desiredDriveState) {
  std::stringstream cmdlineOutput;
  utils::Regex driveNameRegex(regex);

  const auto tapeDriveNames =
    ctx.catalogue.DriveConfig()->getTapeDriveNamesForSchedulerBackend(ctx.scheduler.getSchedulerBackendName());
  bool is_found = false;

  for (const auto& tapeDriveName : tapeDriveNames) {
    const auto regexResult = driveNameRegex.exec(tapeDriveName);
    if (!regexResult.empty()) {
      is_found = true;
      ctx.scheduler.setDesiredDriveState(tapeDriveName, desiredDriveState, ctx.lc);

      cmdlineOutput << "Drive " << tapeDriveName << ": set ";
      if (!desiredDriveState.comment) {
        cmdlineOutput << (desiredDriveState.up ? "Up" : "Down") << (desiredDriveState.forceDown ? " (forced)" : "")
                      << "." << std::endl;
      } else {
        // We modified the comment so we will output that the comment was changed
        cmdlineOutput << "a new comment." << std::endl;
      }
    }
  }
  if (!is_found) {
    cmdlineOutput << "No drives match \"" << regex << "\". No action was taken." << std::endl;
  }

  return cmdlineOutput.str();
}

}  // namespace

void setDriveUp(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  std::optional<std::string> reason = opts.getOptional(OptionString::REASON);
  common::dataStructures::DesiredDriveState desiredDS;
  desiredDS.up = true;
  desiredDS.forceDown = false;
  desiredDS.reason = reason;
  if (!desiredDS.reason) {
    //If reason not provided while setting the drive up, we delete it, so we set it to an empty string
    desiredDS.reason = "";
  }

  std::string cmdlineOutput = setDriveState(ctx, '^' + opts.getRequired(OptionString::DRIVE) + '$', desiredDS);

  response.set_message_txt(cmdlineOutput);
}

void setDriveDown(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  std::string reason = opts.getRequired(OptionString::REASON);
  if (utils::trimString(reason).empty()) {
    throw exception::UserError("You must provide a reason in order to set the drive down");
  }
  common::dataStructures::DesiredDriveState desiredDS;
  desiredDS.up = false;
  desiredDS.forceDown = opts.hasFlag(OptionBoolean::FORCE);
  desiredDS.reason = reason;

  std::string cmdlineOutput = setDriveState(ctx, '^' + opts.getRequired(OptionString::DRIVE) + '$', desiredDS);

  response.set_message_txt(cmdlineOutput);
}

void changeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  std::string comment = opts.getRequired(OptionString::COMMENT);
  if (utils::trimString(comment).empty()) {
    throw exception::UserError("You must provide a comment to change it.");
  }

  common::dataStructures::DesiredDriveState desiredDS;
  desiredDS.comment = comment;

  std::string cmdlineOutput = setDriveState(ctx, '^' + opts.getRequired(OptionString::DRIVE) + '$', desiredDS);

  response.set_message_txt(cmdlineOutput);
}

void removeDrive(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  std::stringstream cmdlineOutput;

  auto regex = opts.getRequired(OptionString::DRIVE);
  regex = '^' + regex + '$';
  utils::Regex driveNameRegex(regex);

  const auto tapeDriveNames =
    ctx.catalogue.DriveConfig()->getTapeDriveNamesForSchedulerBackend(ctx.scheduler.getSchedulerBackendName());
  bool drivesFound = false;

  for (const auto& tapeDriveName : tapeDriveNames) {
    const auto regexResult = driveNameRegex.exec(tapeDriveName);
    if (!regexResult.empty()) {
      const auto tapeDriveOpt = ctx.catalogue.DriveState()->getTapeDrive(tapeDriveName);
      if (!tapeDriveOpt.has_value()) {
        continue;
      }
      if (const auto& tapeDrive = tapeDriveOpt.value();
          tapeDrive.driveStatus == common::dataStructures::DriveStatus::Down
          || tapeDrive.driveStatus == common::dataStructures::DriveStatus::Shutdown
          || tapeDrive.driveStatus == common::dataStructures::DriveStatus::Unknown
          || opts.hasFlag(OptionBoolean::FORCE)) {
        ctx.scheduler.removeDrive(tapeDriveName, ctx.lc);
        cmdlineOutput << "Drive " << tapeDriveName << " removed"
                      << (opts.hasFlag(OptionBoolean::FORCE) ? " (forced)." : ".") << std::endl;
      } else {
        cmdlineOutput << "Drive " << tapeDriveName << " in state "
                      << common::dataStructures::toString(tapeDrive.driveStatus) << " and force is not set (skipped)."
                      << std::endl;
      }
      drivesFound = true;
    }
  }

  if (!drivesFound) {
    cmdlineOutput << "No drives match \"" << regex << "\". No drives were removed." << std::endl;
  }

  response.set_message_txt(cmdlineOutput.str());
}

void removeFailedRequest(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& objectId = opts.getRequired(OptionString::OBJECTID);

  ctx.scheduler.deleteFailed(objectId, ctx.lc);
}

void addRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  // VIDs can be provided as a single option or as a list
  std::vector<std::string> vid_list;
  std::string bufferURL;

  if (const auto vidl = opts.getOptional(OptionStrList::VID); vidl.has_value()) {
    vid_list = vidl.value();
  }
  if (const auto vid = opts.getOptional(OptionString::VID); vid.has_value()) {
    vid_list.push_back(vid.value());
  }

  if (vid_list.empty()) {
    throw exception::UserError("Must specify at least one vid, using --vid or --vidfile options");
  }

  // Check if there is a default repack VO
  if (!ctx.catalogue.VO()->getDefaultVirtualOrganizationForRepack().has_value()) {
    throw exception::UserError("There is no default virtual organization for repack.");
  }

  auto mountPolicyProvidedByUser = opts.getRequired(OptionString::MOUNT_POLICY);

  //Get the mountpolicy from the catalogue
  common::dataStructures::MountPolicy mountPolicy;
  {
    const auto mountPolicies = ctx.catalogue.MountPolicy()->getMountPolicies();
    auto repackMountPolicyItor =
      std::find_if(mountPolicies.begin(),
                   mountPolicies.end(),
                   [&mountPolicyProvidedByUser](const common::dataStructures::MountPolicy& mp) {
                     return mp.name == mountPolicyProvidedByUser;
                   });
    if (repackMountPolicyItor != mountPolicies.end()) {
      //The mount policy exists
      mountPolicy = *repackMountPolicyItor;
    } else {
      //The mount policy does not exist, throw a user error
      throw exception::UserError("The mount policy name provided does not match any existing mount policy.");
    }
  }
  if (const auto buff = opts.getOptional(OptionString::BUFFERURL); buff.has_value()) {
    //The buffer is provided by the user
    bufferURL = buff.value();
  } else {
    //Buffer is not provided by the user, try to get the one from the configuration file
    if (ctx.defaults.repack.buffer_url.has_value()) {
      bufferURL = ctx.defaults.repack.buffer_url.value();
    } else {
      //Buffer is neither provided by the user, neither provided by the frontend configuration file, exception
      throw exception::UserError(
        "Must specify the buffer URL using --bufferurl option or using the frontend configuration file.");
    }
  }

  // Expand, repack, or both ?
  common::dataStructures::RepackInfo::Type type;

  if (opts.hasFlag(OptionBoolean::JUSTADDCOPIES) && opts.hasFlag(OptionBoolean::JUSTMOVE)) {
    throw exception::UserError("--justaddcopies and --justmove are mutually exclusive");
  } else if (opts.hasFlag(OptionBoolean::JUSTADDCOPIES)) {
    type = common::dataStructures::RepackInfo::Type::AddCopiesOnly;
  } else if (opts.hasFlag(OptionBoolean::JUSTMOVE)) {
    type = common::dataStructures::RepackInfo::Type::MoveOnly;
  } else {
    type = common::dataStructures::RepackInfo::Type::MoveAndAddCopies;
  }

  bool noRecall = opts.hasFlag(OptionBoolean::NO_RECALL);
  const auto maxFilesToSelectOpt = opts.getOptional(OptionUInt64::MAX_FILES_TO_EXPAND);
  // 1. If the 'MAX_FILES_TO_EXPAND' parameter was received, use its value.
  // 2. Else, if the 'defaults.repack.max_files_to_select' configuration option is defined, use its value.
  // 3. Else, use 0 as a default. Zero means that all files will be expanded.
  uint64_t maxFilesToSelect =
    maxFilesToSelectOpt ? maxFilesToSelectOpt.value() : ctx.defaults.repack.max_files_to_select.value_or(0);

  // Process each item in the list
  for (auto it = vid_list.begin(); it != vid_list.end(); ++it) {
    SchedulerDatabase::QueueRepackRequest repackRequest(*it, bufferURL, type, mountPolicy, noRecall, maxFilesToSelect);
    ctx.scheduler.queueRepack(ctx.identity, repackRequest, ctx.lc);
  }
}

void removeRepack(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);

  ctx.scheduler.cancelRepack(ctx.identity, vid, ctx.lc);
}

void showRepackErrors(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  response.set_message_txt("Not implemented");
}

}  // namespace cta::admin_api::commands::sync::handlers
