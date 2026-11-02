/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/converters/EntryLogHelpers.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

namespace cta::admin_api::commands::stream {

void toProto(const common::dataStructures::DiskInstance& diskInstance,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_dils_item();

  item->set_name(diskInstance.name);
  item->set_instance_name(instanceName);
  fillEntryLogs(item, diskInstance.creationLog, diskInstance.lastModificationLog);
  item->set_comment(diskInstance.comment);
}

void toProto(const common::dataStructures::DiskInstanceSpace& diskInstanceSpace,
             cta::xrd::Data& data,
             [[maybe_unused]] const std::string& instanceName) {
  auto item = data.mutable_disls_item();

  item->set_name(diskInstanceSpace.name);
  // This has always reported the name of the space, not the name of the instance of the admin API
  item->set_instance_name(diskInstanceSpace.name);
  item->set_disk_instance(diskInstanceSpace.diskInstance);
  item->set_refresh_interval(diskInstanceSpace.refreshInterval);
  item->set_free_space_query_url(diskInstanceSpace.freeSpaceQueryURL);
  item->set_free_space(diskInstanceSpace.freeSpace);
  fillEntryLogs(item, diskInstanceSpace.creationLog, diskInstanceSpace.lastModificationLog);
  item->set_comment(diskInstanceSpace.comment);
}

void toProto(const disk::DiskSystem& diskSystem, cta::xrd::Data& data, const std::string& instanceName) {
  auto item = data.mutable_dsls_item();

  item->set_name(diskSystem.name);
  item->set_instance_name(instanceName);
  item->set_file_regexp(diskSystem.fileRegexp);
  item->set_disk_instance(diskSystem.diskInstanceSpace.diskInstance);
  item->set_disk_instance_space(diskSystem.diskInstanceSpace.name);
  item->set_targeted_free_space(diskSystem.targetedFreeSpace);
  item->set_sleep_time(diskSystem.sleepTime);
  fillEntryLogs(item, diskSystem.creationLog, diskSystem.lastModificationLog);
  item->set_comment(diskSystem.comment);
}

}  // namespace cta::admin_api::commands::stream
