/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/AdminCmdOptions.hpp"
#include "commands/stream/converters/EntryLogHelpers.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

#include <algorithm>

namespace cta::admin_api::commands::stream {

void toProto(const catalogue::MediaTypeWithLogs& mediaType, cta::xrd::Data& data, const std::string& instanceName) {
  auto item = data.mutable_mtls_item();

  item->set_name(mediaType.name);
  item->set_instance_name(instanceName);
  item->set_cartridge(mediaType.cartridge);
  item->set_capacity(mediaType.capacityInBytes);
  if (mediaType.primaryDensityCode.has_value()) {
    item->set_primary_density_code(mediaType.primaryDensityCode.value());
  }
  if (mediaType.secondaryDensityCode.has_value()) {
    item->set_secondary_density_code(mediaType.secondaryDensityCode.value());
  }
  if (mediaType.nbWraps.has_value()) {
    item->set_number_of_wraps(mediaType.nbWraps.value());
  }
  if (mediaType.minLPos.has_value()) {
    item->set_min_lpos(mediaType.minLPos.value());
  }
  if (mediaType.maxLPos.has_value()) {
    item->set_max_lpos(mediaType.maxLPos.value());
  }
  item->set_comment(mediaType.comment);
  fillEntryLogs(item, mediaType.creationLog, mediaType.lastModificationLog);
}

void toProto(const common::dataStructures::LogicalLibrary& library,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_llls_item();

  item->set_name(library.name);
  item->set_is_disabled(library.isDisabled);
  if (library.physicalLibraryName) {
    item->set_physical_library(library.physicalLibraryName.value());
  }
  if (library.disabledReason) {
    item->set_disabled_reason(library.disabledReason.value());
  }
  fillEntryLogs(item, library.creationLog, library.lastModificationLog);
  item->set_comment(library.comment);
  item->set_instance_name(instanceName);
}

void toProto(const common::dataStructures::PhysicalLibrary& library,
             cta::xrd::Data& data,
             const std::string& instanceName) {
  auto item = data.mutable_plls_item();

  item->set_name(library.name);
  item->set_instance_name(instanceName);
  item->set_manufacturer(library.manufacturer);
  item->set_model(library.model);
  if (library.type.has_value()) {
    item->set_type(library.type.value());
  }
  if (library.guiUrl.has_value()) {
    item->set_gui_url(library.guiUrl.value());
  }
  if (library.webcamUrl.has_value()) {
    item->set_webcam_url(library.webcamUrl.value());
  }
  if (library.location.has_value()) {
    item->set_location(library.location.value());
  }
  if (library.nbAvailableCartridgeSlots.has_value()) {
    item->set_nb_available_cartridge_slots(library.nbAvailableCartridgeSlots.value());
  }
  if (library.comment.has_value()) {
    item->set_comment(library.comment.value());
  }
  if (library.disabledReason.has_value()) {
    item->set_disabled_reason(library.disabledReason.value());
  }
  item->set_nb_physical_cartridge_slots(library.nbPhysicalCartridgeSlots);
  item->set_nb_physical_drive_slots(library.nbPhysicalDriveSlots);
  fillEntryLogs(item, library.creationLog, library.lastModificationLog);
  item->set_is_disabled(library.isDisabled);
}

std::vector<common::dataStructures::LogicalLibrary> loadLogicalLibraries(catalogue::Catalogue& catalogue,
                                                                         const admin::AdminCmd& adminCmd) {
  const commands::AdminCmdOptions request(adminCmd);
  const std::optional<bool> disabled = request.getOptional(admin::OptionBoolean::DISABLED);

  auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
  if (disabled.has_value()) {
    std::erase_if(libraries, [&disabled](const auto& library) { return library.isDisabled != disabled.value(); });
  }
  return libraries;
}

}  // namespace cta::admin_api::commands::stream
