/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/handlers/Handlers.hpp"
#include "common/exception/UserError.hpp"

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

void addDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_SYSTEM);
  const auto& diskInstance = opts.getRequired(OptionString::DISK_INSTANCE);
  const auto& diskInstanceSpace = opts.getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto& fileRegexp = opts.getRequired(OptionString::FILE_REGEXP);
  const auto& targetedFreeSpace = opts.getRequired(OptionUInt64::TARGETED_FREE_SPACE);
  const auto& sleepTime = opts.getRequired(OptionUInt64::SLEEP_TIME);
  const auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.DiskSystem()->createDiskSystem(ctx.identity,
                                               name,
                                               diskInstance,
                                               diskInstanceSpace,
                                               fileRegexp,
                                               targetedFreeSpace,
                                               sleepTime,
                                               comment);
}

void removeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_SYSTEM);

  ctx.catalogue.DiskSystem()->deleteDiskSystem(name);
}

void changeDiskSystem(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_SYSTEM);
  const auto& fileRegexp = opts.getOptional(OptionString::FILE_REGEXP);
  const auto targetedFreeSpace = opts.getOptional(OptionUInt64::TARGETED_FREE_SPACE);
  const auto sleepTime = opts.getOptional(OptionUInt64::SLEEP_TIME);
  const auto comment = opts.getOptional(OptionString::COMMENT);
  const auto diskInstanceName = opts.getOptional(OptionString::DISK_INSTANCE);
  const auto diskInstanceSpaceName = opts.getOptional(OptionString::DISK_INSTANCE_SPACE);

  if (comment.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemComment(ctx.identity, name, comment.value());
  }
  if (fileRegexp.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemFileRegexp(ctx.identity, name, fileRegexp.value());
  }
  if (sleepTime.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemSleepTime(ctx.identity, name, sleepTime.value());
  }
  if (targetedFreeSpace.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemTargetedFreeSpace(ctx.identity, name, targetedFreeSpace.value());
  }
  if (diskInstanceName.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemDiskInstanceName(ctx.identity, name, diskInstanceName.value());
  }
  if (diskInstanceSpaceName.has_value()) {
    ctx.catalogue.DiskSystem()->modifyDiskSystemDiskInstanceSpaceName(ctx.identity,
                                                                      name,
                                                                      diskInstanceSpaceName.value());
  }
}

void addDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE);
  const auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.DiskInstance()->createDiskInstance(ctx.identity, name, comment);
}

void removeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE);

  ctx.catalogue.DiskInstance()->deleteDiskInstance(name);
}

void changeDiskInstance(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE);

  if (const auto comment = opts.getOptional(OptionString::COMMENT); comment.has_value()) {
    ctx.catalogue.DiskInstance()->modifyDiskInstanceComment(ctx.identity, name, comment.value());
  }
}

void addDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto& diskInstance = opts.getRequired(OptionString::DISK_INSTANCE);
  const auto& comment = opts.getRequired(OptionString::COMMENT);
  const auto& freeSpaceQueryURL = opts.getRequired(OptionString::FREE_SPACE_QUERY_URL);
  const auto refreshInterval = opts.getRequired(OptionUInt64::REFRESH_INTERVAL);

  ctx.catalogue.DiskInstanceSpace()
    ->createDiskInstanceSpace(ctx.identity, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);
}

void removeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto& diskInstance = opts.getRequired(OptionString::DISK_INSTANCE);

  ctx.catalogue.DiskInstanceSpace()->deleteDiskInstanceSpace(name, diskInstance);
}

void changeDiskInstanceSpace(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto& diskInstance = opts.getRequired(OptionString::DISK_INSTANCE);
  const auto comment = opts.getOptional(OptionString::COMMENT);
  const auto& freeSpaceQueryURL = opts.getOptional(OptionString::FREE_SPACE_QUERY_URL);
  const auto refreshInterval = opts.getOptional(OptionUInt64::REFRESH_INTERVAL);

  if (comment.has_value()) {
    ctx.catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceComment(ctx.identity,
                                                                      name,
                                                                      diskInstance,
                                                                      comment.value());
  }
  if (freeSpaceQueryURL.has_value()) {
    ctx.catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(ctx.identity,
                                                                       name,
                                                                       diskInstance,
                                                                       freeSpaceQueryURL.value());
  }
  if (refreshInterval.has_value()) {
    ctx.catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(ctx.identity,
                                                                              name,
                                                                              diskInstance,
                                                                              refreshInterval.value());
  }
}

}  // namespace cta::admin_api::commands::sync::handlers
