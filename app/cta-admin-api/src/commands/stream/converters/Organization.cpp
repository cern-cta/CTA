/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/converters/EntryLogHelpers.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

namespace cta::admin_api::commands::stream {

void toProto(const common::dataStructures::AdminUser& adminUser,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_adls_item();

  item->set_user(adminUser.name);
  fillEntryLogs(item, adminUser.creationLog, adminUser.lastModificationLog);
  item->set_comment(adminUser.comment);
  item->set_instance_name(instanceName);
}

void toProto(const common::dataStructures::VirtualOrganization& vo,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_vols_item();

  item->set_name(vo.name);
  item->set_instance_name(instanceName);
  item->set_read_max_drives(vo.readMaxDrives);
  item->set_write_max_drives(vo.writeMaxDrives);
  item->set_max_file_size(vo.maxFileSize);
  fillEntryLogs(item, vo.creationLog, vo.lastModificationLog);
  item->set_comment(vo.comment);
  item->set_diskinstance(vo.diskInstanceName);
  item->set_is_repack_vo(vo.isRepackVo);
}

}  // namespace cta::admin_api::commands::stream
