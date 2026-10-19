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

void addAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& username = opts.getRequired(OptionString::USERNAME);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.AdminUser()->createAdminUser(ctx.identity, username, comment);
}

void changeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& username = opts.getRequired(OptionString::USERNAME);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.AdminUser()->modifyAdminUserComment(ctx.identity, username, comment);
}

void removeAdmin(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& username = opts.getRequired(OptionString::USERNAME);

  ctx.catalogue.AdminUser()->deleteAdminUser(username);
}

void addVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::VO);
  const auto& readMaxDrives = opts.getRequired(OptionUInt64::READ_MAX_DRIVES);
  const auto& writeMaxDrives = opts.getRequired(OptionUInt64::WRITE_MAX_DRIVES);
  const auto& comment = opts.getRequired(OptionString::COMMENT);
  const auto& maxFileSizeOpt = opts.getOptional(OptionUInt64::MAX_FILE_SIZE);
  const auto& diskInstanceName = opts.getRequired(OptionString::DISK_INSTANCE);
  const auto& isRepackVo = opts.getOptional(OptionBoolean::IS_REPACK_VO);

  common::dataStructures::VirtualOrganization vo;
  vo.name = name;
  vo.readMaxDrives = readMaxDrives;
  vo.writeMaxDrives = writeMaxDrives;
  vo.comment = comment;
  vo.diskInstanceName = diskInstanceName;

  vo.maxFileSize = maxFileSizeOpt.value_or(ctx.archiveFileMaxSize);
  vo.isRepackVo = isRepackVo.value_or(false);

  ctx.catalogue.VO()->createVirtualOrganization(ctx.identity, vo);
}

void changeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::VO);
  const auto readMaxDrives = opts.getOptional(OptionUInt64::READ_MAX_DRIVES);
  const auto writeMaxDrives = opts.getOptional(OptionUInt64::WRITE_MAX_DRIVES);
  const auto comment = opts.getOptional(OptionString::COMMENT);
  const auto maxFileSize = opts.getOptional(OptionUInt64::MAX_FILE_SIZE);
  const auto diskInstanceName = opts.getOptional(OptionString::DISK_INSTANCE);
  const auto isRepackVo = opts.getOptional(OptionBoolean::IS_REPACK_VO);

  if (comment.has_value()) {
    ctx.catalogue.VO()->modifyVirtualOrganizationComment(ctx.identity, name, comment.value());
  }

  if (readMaxDrives.has_value()) {
    ctx.catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(ctx.identity, name, readMaxDrives.value());
  }

  if (writeMaxDrives.has_value()) {
    ctx.catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(ctx.identity, name, writeMaxDrives.value());
  }

  if (maxFileSize.has_value()) {
    ctx.catalogue.VO()->modifyVirtualOrganizationMaxFileSize(ctx.identity, name, maxFileSize.value());
  }

  if (diskInstanceName.has_value()) {
    ctx.catalogue.VO()->modifyVirtualOrganizationDiskInstanceName(ctx.identity, name, diskInstanceName.value());
  }

  if (isRepackVo.has_value()) {
    // Don't allow unsetting repackvo while repacks are ongoing.
    if (ctx.scheduler.repackExists() && !isRepackVo.value()) {
      throw exception::UserError("Cannot remove default virtual organization "
                                 "for repack while repacks are ongoing.");
    }
    ctx.catalogue.VO()->modifyVirtualOrganizationIsRepackVo(ctx.identity, name, isRepackVo.value());
  }
}

void removeVirtualOrganization(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::VO);

  if (const auto defaultRepackVo = ctx.catalogue.VO()->getDefaultVirtualOrganizationForRepack();
      defaultRepackVo && (defaultRepackVo->name == name) && ctx.scheduler.repackExists()) {
    throw exception::UserError("Cannot remove default virtual organization for repack while repacks are ongoing.");
  }

  ctx.catalogue.VO()->deleteVirtualOrganization(name);
}

}  // namespace cta::admin_api::commands::sync::handlers
