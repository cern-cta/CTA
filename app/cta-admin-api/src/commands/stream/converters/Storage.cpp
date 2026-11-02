/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/TapePoolSearchCriteria.hpp"
#include "commands/AdminCmdOptions.hpp"
#include "commands/stream/converters/EntryLogHelpers.hpp"
#include "commands/stream/converters/StreamConverters.hpp"
#include "common/exception/UserError.hpp"
#include "protobuf/admin/ArchiveRouteTypeSerDeser.hpp"

namespace cta::admin_api::commands::stream {

void toProto(const common::dataStructures::StorageClass& storageClass,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_scls_item();

  item->set_name(storageClass.name);
  item->set_nb_copies(storageClass.nbCopies);
  item->set_vo(storageClass.vo.name);
  fillEntryLogs(item, storageClass.creationLog, storageClass.lastModificationLog);
  item->set_comment(storageClass.comment);
  item->set_instance_name(instanceName);
}

void toProto(const catalogue::TapePool& tapePool, cta::xrd::Data& data, const std::string& instanceName) {
  auto item = data.mutable_tpls_item();

  item->set_name(tapePool.name);
  item->set_instance_name(instanceName);
  item->set_vo(tapePool.vo.name);
  item->set_num_tapes(tapePool.nbTapes);
  item->set_num_partial_tapes(tapePool.nbPartialTapes);
  item->set_num_physical_files(tapePool.nbPhysicalFiles);
  item->set_capacity_bytes(tapePool.capacityBytes);
  item->set_data_bytes(tapePool.dataBytes);
  item->set_encrypt(tapePool.encryption);
  item->set_encryption_key_name(tapePool.encryptionKeyName.value_or(""));
  // The tape pools do not use the creation_log/last_modification_log field names
  fillEntryLog(item->mutable_created(), tapePool.creationLog);
  fillEntryLog(item->mutable_modified(), tapePool.lastModificationLog);
  item->set_comment(tapePool.comment);
  for (const auto& source : tapePool.supply_source_set) {
    item->add_supply_source(source);
  }
  for (const auto& destination : tapePool.supply_destination_set) {
    item->add_supply_destination(destination);
  }
}

void toProto(const common::dataStructures::ArchiveRoute& route, cta::xrd::Data& data, const std::string& instanceName) {
  auto item = data.mutable_arls_item();

  item->set_storage_class(route.storageClassName);
  item->set_copy_number(route.copyNb);
  item->set_archive_route_type(cta::admin::ArchiveRouteTypeFormatToProtobuf(route.type));
  item->set_tapepool(route.tapePoolName);
  fillEntryLogs(item, route.creationLog, route.lastModificationLog);
  item->set_comment(route.comment);
  item->set_instance_name(instanceName);
}

std::vector<common::dataStructures::StorageClass> loadStorageClasses(catalogue::Catalogue& catalogue,
                                                                     const admin::AdminCmd& adminCmd) {
  const commands::AdminCmdOptions request(adminCmd);

  const std::optional<std::string> storageClassName = request.getOptional(admin::OptionString::STORAGE_CLASS);
  const std::optional<std::string> vid = request.getOptional(admin::OptionString::VID);

  if (storageClassName.has_value() && vid.has_value()) {
    throw exception::UserError("Cannot specify both storage class name and VID");
  }

  if (storageClassName.has_value()) {
    return {catalogue.StorageClass()->getStorageClass(storageClassName.value())};
  }
  if (vid.has_value()) {
    return catalogue.StorageClass()->getStorageClassesByVid(vid.value());
  }
  return catalogue.StorageClass()->getStorageClasses();
}

std::vector<catalogue::TapePool> loadTapePools(const catalogue::Catalogue& catalogue, const admin::AdminCmd& adminCmd) {
  const commands::AdminCmdOptions request(adminCmd);

  catalogue::TapePoolSearchCriteria searchCriteria;
  searchCriteria.name = request.getOptional(admin::OptionString::TAPE_POOL);
  searchCriteria.vo = request.getOptional(admin::OptionString::VO);
  searchCriteria.encrypted = request.getOptional(admin::OptionBoolean::ENCRYPTED);
  searchCriteria.encryptionKeyName = request.getOptional(admin::OptionString::ENCRYPTION_KEY_NAME);

  if (searchCriteria.encrypted.has_value() && searchCriteria.encryptionKeyName.has_value()) {
    throw exception::UserError("Do not request both '--encrypted' and '--encryptionkeyname' at same time.");
  }

  return catalogue.TapePool()->getTapePools(searchCriteria);
}

}  // namespace cta::admin_api::commands::stream
