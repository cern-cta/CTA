/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/converters/EntryLogHelpers.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

namespace cta::admin_api::commands::stream {

void toProto(const common::dataStructures::RequesterActivityMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_amrls_item();

  item->set_disk_instance(rule.diskInstance);
  item->set_activity_mount_rule(rule.name);
  item->set_mount_policy(rule.mountPolicy);
  item->set_activity_regex(rule.activityRegex);
  fillEntryLogs(item, rule.creationLog, rule.lastModificationLog);
  item->set_comment(rule.comment);
  item->set_instance_name(instanceName);
}

void toProto(const common::dataStructures::RequesterGroupMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_gmrls_item();

  item->set_instance_name(instanceName);
  item->set_disk_instance(rule.diskInstance);
  item->set_group_mount_rule(rule.name);
  item->set_mount_policy(rule.mountPolicy);
  fillEntryLogs(item, rule.creationLog, rule.lastModificationLog);
  item->set_comment(rule.comment);
}

void toProto(const common::dataStructures::RequesterMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_rmrls_item();

  item->set_instance_name(instanceName);
  item->set_disk_instance(rule.diskInstance);
  item->set_requester_mount_rule(rule.name);
  item->set_mount_policy(rule.mountPolicy);
  fillEntryLogs(item, rule.creationLog, rule.lastModificationLog);
  item->set_comment(rule.comment);
}

void toProto(const common::dataStructures::MountPolicy& mountPolicy,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_mpls_item();

  item->set_name(mountPolicy.name);
  item->set_instance_name(instanceName);
  item->set_archive_priority(mountPolicy.archivePriority);
  item->set_archive_min_request_age(mountPolicy.archiveMinRequestAge);
  item->set_retrieve_priority(mountPolicy.retrievePriority);
  item->set_retrieve_min_request_age(mountPolicy.retrieveMinRequestAge);
  fillEntryLogs(item, mountPolicy.creationLog, mountPolicy.lastModificationLog);
  item->set_comment(mountPolicy.comment);
}

}  // namespace cta::admin_api::commands::stream
