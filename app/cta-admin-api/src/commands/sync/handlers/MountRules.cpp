/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "commands/sync/handlers/Handlers.hpp"
#include "common/exception/UserError.hpp"

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

std::optional<std::string> addGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& mountpolicy = opts.getRequired(OptionString::MOUNT_POLICY);
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.RequesterGroupMountRule()->createRequesterGroupMountRule(ctx.identity, mountpolicy, in, name, comment);
  return std::nullopt;
}

std::optional<std::string> changeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);

  if (const auto comment = opts.getOptional(OptionString::COMMENT); comment) {
    ctx.catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(ctx.identity,
                                                                                  in,
                                                                                  name,
                                                                                  comment.value());
  }
  if (const auto mountpolicy = opts.getOptional(OptionString::MOUNT_POLICY); mountpolicy) {
    ctx.catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(ctx.identity,
                                                                                 in,
                                                                                 name,
                                                                                 mountpolicy.value());
  }
  return std::nullopt;
}

std::optional<std::string> removeGroupMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);

  ctx.catalogue.RequesterGroupMountRule()->deleteRequesterGroupMountRule(in, name);
  return std::nullopt;
}

std::optional<std::string> addMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& group = opts.getRequired(OptionString::MOUNT_POLICY);
  auto& archivepriority = opts.getRequired(OptionUInt64::ARCHIVE_PRIORITY);
  auto& minarchiverequestage = opts.getRequired(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE);
  auto& retrievepriority = opts.getRequired(OptionUInt64::RETRIEVE_PRIORITY);
  auto& minretrieverequestage = opts.getRequired(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  catalogue::CreateMountPolicyAttributes mountPolicy;
  mountPolicy.name = group;
  mountPolicy.archivePriority = archivepriority;
  mountPolicy.minArchiveRequestAge = minarchiverequestage;
  mountPolicy.retrievePriority = retrievepriority;
  mountPolicy.minRetrieveRequestAge = minretrieverequestage;
  mountPolicy.comment = comment;

  ctx.catalogue.MountPolicy()->createMountPolicy(ctx.identity, mountPolicy);
  return std::nullopt;
}

std::optional<std::string> changeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& group = opts.getRequired(OptionString::MOUNT_POLICY);
  auto archivepriority = opts.getOptional(OptionUInt64::ARCHIVE_PRIORITY);
  auto minarchiverequestage = opts.getOptional(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE);
  auto retrievepriority = opts.getOptional(OptionUInt64::RETRIEVE_PRIORITY);
  auto minretrieverequestage = opts.getOptional(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE);
  auto comment = opts.getOptional(OptionString::COMMENT);

  if (archivepriority.has_value()) {
    ctx.catalogue.MountPolicy()->modifyMountPolicyArchivePriority(ctx.identity, group, archivepriority.value());
  }
  if (minarchiverequestage.has_value()) {
    ctx.catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(ctx.identity,
                                                                       group,
                                                                       minarchiverequestage.value());
  }
  if (retrievepriority.has_value()) {
    ctx.catalogue.MountPolicy()->modifyMountPolicyRetrievePriority(ctx.identity, group, retrievepriority.value());
  }
  if (minretrieverequestage.has_value()) {
    ctx.catalogue.MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(ctx.identity,
                                                                        group,
                                                                        minretrieverequestage.value());
  }
  if (comment.has_value()) {
    ctx.catalogue.MountPolicy()->modifyMountPolicyComment(ctx.identity, group, comment.value());
  }
  return std::nullopt;
}

std::optional<std::string> removeMountPolicy(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& group = opts.getRequired(OptionString::MOUNT_POLICY);

  ctx.catalogue.MountPolicy()->deleteMountPolicy(group);
  return std::nullopt;
}

std::optional<std::string> addRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& mountpolicy = opts.getRequired(OptionString::MOUNT_POLICY);
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.RequesterMountRule()->createRequesterMountRule(ctx.identity, mountpolicy, in, name, comment);
  return std::nullopt;
}

std::optional<std::string> changeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto mountpolicy = opts.getOptional(OptionString::MOUNT_POLICY);

  if (comment.has_value()) {
    ctx.catalogue.RequesterMountRule()->modifyRequesterMountRuleComment(ctx.identity, in, name, comment.value());
  }
  if (mountpolicy.has_value()) {
    ctx.catalogue.RequesterMountRule()->modifyRequesterMountRulePolicy(ctx.identity, in, name, mountpolicy.value());
  }
  return std::nullopt;
}

std::optional<std::string> removeRequesterMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);

  ctx.catalogue.RequesterMountRule()->deleteRequesterMountRule(in, name);
  return std::nullopt;
}

std::optional<std::string> addActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& mountpolicy = opts.getRequired(OptionString::MOUNT_POLICY);
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto& comment = opts.getRequired(OptionString::COMMENT);
  auto& activityRegex = opts.getRequired(OptionString::ACTIVITY_REGEX);

  ctx.catalogue.RequesterActivityMountRule()
    ->createRequesterActivityMountRule(ctx.identity, mountpolicy, in, name, activityRegex, comment);
  return std::nullopt;
}

std::optional<std::string> changeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto& activityRegex = opts.getRequired(OptionString::ACTIVITY_REGEX);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto mountpolicy = opts.getOptional(OptionString::MOUNT_POLICY);

  if (comment.has_value()) {
    ctx.catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(ctx.identity,
                                                                                        in,
                                                                                        name,
                                                                                        activityRegex,
                                                                                        comment.value());
  }
  if (mountpolicy.has_value()) {
    ctx.catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(ctx.identity,
                                                                                       in,
                                                                                       name,
                                                                                       activityRegex,
                                                                                       mountpolicy.value());
  }
  return std::nullopt;
}

std::optional<std::string> removeActivityMountRule(const SyncCmdContext& ctx, const AdminCmdOptions& opts) {
  auto& in = opts.getRequired(OptionString::INSTANCE);
  auto& name = opts.getRequired(OptionString::USERNAME);
  auto& activityRegex = opts.getRequired(OptionString::ACTIVITY_REGEX);

  ctx.catalogue.RequesterActivityMountRule()->deleteRequesterActivityMountRule(in, name, activityRegex);
  return std::nullopt;
}

}  // namespace cta::admin_api::commands::sync::handlers
