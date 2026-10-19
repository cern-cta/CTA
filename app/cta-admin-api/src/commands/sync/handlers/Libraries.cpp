/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/MediaType.hpp"
#include "commands/sync/handlers/Handlers.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/exception/UserError.hpp"

#include <limits>

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

void addLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::LOGICAL_LIBRARY);
  auto isDisabled = opts.getOptional(OptionBoolean::DISABLED);
  auto physicalLibraryName = opts.getOptional(OptionString::PHYSICAL_LIBRARY);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  ctx.catalogue.LogicalLibrary()->createLogicalLibrary(ctx.identity,
                                                       name,
                                                       isDisabled.value_or(false),
                                                       physicalLibraryName,
                                                       comment);
}

void changeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::LOGICAL_LIBRARY);
  auto disabled = opts.getOptional(OptionBoolean::DISABLED);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto disabledReason = opts.getOptional(OptionString::DISABLED_REASON);
  auto physicalLibrary = opts.getOptional(OptionString::PHYSICAL_LIBRARY);

  if (disabled.has_value()) {
    if (disabled.value() && !disabledReason) {
      throw exception::UserError(std::string("Cannot disable logical library ") + name
                                 + " because the reason has not been provided");
    }
    ctx.catalogue.LogicalLibrary()->setLogicalLibraryDisabled(ctx.identity, name, disabled.value());
    if ((!disabled.value()) && (!disabledReason)) {
      // if enabling the tape and the reason is not specified in the command, erase the reason
      ctx.catalogue.LogicalLibrary()->modifyLogicalLibraryDisabledReason(ctx.identity, name, "");
    }
  }
  if (comment.has_value()) {
    ctx.catalogue.LogicalLibrary()->modifyLogicalLibraryComment(ctx.identity, name, comment.value());
  }
  if (disabledReason.has_value()) {
    ctx.catalogue.LogicalLibrary()->modifyLogicalLibraryDisabledReason(ctx.identity, name, disabledReason.value());
  }
  if (physicalLibrary.has_value()) {
    ctx.catalogue.LogicalLibrary()->modifyLogicalLibraryPhysicalLibrary(ctx.identity, name, physicalLibrary.value());
  }
}

void removeLogicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::LOGICAL_LIBRARY);

  ctx.catalogue.LogicalLibrary()->deleteLogicalLibrary(name);
}

void addMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  // Bounds check unsigned integer options less than 64-bits in width
  const auto nbWraps = opts.getOptional(OptionUInt64::NUMBER_OF_WRAPS);
  const auto primaryDensityCode = opts.getOptional(OptionUInt64::PRIMARY_DENSITY_CODE);
  const auto secondaryDensityCode = opts.getOptional(OptionUInt64::SECONDARY_DENSITY_CODE);
  if (nbWraps.has_value() && nbWraps.value() > std::numeric_limits<uint32_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Number of wraps cannot be larger than " << std::numeric_limits<uint32_t>::max()
                    << ": value=" << nbWraps.value();
    throw ex;
  }
  if (primaryDensityCode.has_value() && primaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Primary density code cannot be larger than " << (uint16_t) (std::numeric_limits<uint8_t>::max())
                    << ": value=" << primaryDensityCode.value();
    throw ex;
  }
  if (secondaryDensityCode.has_value() && secondaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Secondary density code cannot be larger than "
                    << (uint16_t) (std::numeric_limits<uint8_t>::max()) << ": value=" << secondaryDensityCode.value();
    throw ex;
  }

  catalogue::MediaType mediaType;
  mediaType.name = opts.getRequired(OptionString::MEDIA_TYPE);
  mediaType.cartridge = opts.getRequired(OptionString::CARTRIDGE);
  mediaType.capacityInBytes = opts.getRequired(OptionUInt64::CAPACITY);
  if (primaryDensityCode.has_value()) {
    mediaType.primaryDensityCode = primaryDensityCode.value();
  }
  if (secondaryDensityCode.has_value()) {
    mediaType.secondaryDensityCode = secondaryDensityCode.value();
  }
  if (nbWraps.has_value()) {
    mediaType.nbWraps = nbWraps.value();
  }
  mediaType.minLPos = opts.getOptional(OptionUInt64::MIN_LPOS);
  mediaType.maxLPos = opts.getOptional(OptionUInt64::MAX_LPOS);
  mediaType.comment = opts.getRequired(OptionString::COMMENT);
  ctx.catalogue.MediaType()->createMediaType(ctx.identity, mediaType);
}

void changeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& mediaTypeName = opts.getRequired(OptionString::MEDIA_TYPE);
  auto cartridge = opts.getOptional(OptionString::CARTRIDGE);
  auto primaryDensityCode = opts.getOptional(OptionUInt64::PRIMARY_DENSITY_CODE);
  auto secondaryDensityCode = opts.getOptional(OptionUInt64::SECONDARY_DENSITY_CODE);
  auto nbWraps = opts.getOptional(OptionUInt64::NUMBER_OF_WRAPS);
  auto minlpos = opts.getOptional(OptionUInt64::MIN_LPOS);
  auto maxlpos = opts.getOptional(OptionUInt64::MAX_LPOS);
  auto comment = opts.getOptional(OptionString::COMMENT);

  // Bounds check unsigned integer options less than 64-bits in width
  if (nbWraps.has_value() && nbWraps.value() > std::numeric_limits<uint32_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Number of wraps cannot be larger than " << std::numeric_limits<uint32_t>::max()
                    << ": value=" << nbWraps.value();
    throw ex;
  }
  if (primaryDensityCode.has_value() && primaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Primary density code cannot be larger than " << (uint16_t) (std::numeric_limits<uint8_t>::max())
                    << ": value=" << primaryDensityCode.value();
    throw ex;
  }
  if (secondaryDensityCode.has_value() && secondaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Secondary density code cannot be larger than "
                    << (uint16_t) (std::numeric_limits<uint8_t>::max()) << ": value=" << secondaryDensityCode.value();
    throw ex;
  }

  if (cartridge.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypeCartridge(ctx.identity, mediaTypeName, cartridge.value());
  }
  if (primaryDensityCode.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypePrimaryDensityCode(ctx.identity,
                                                                 mediaTypeName,
                                                                 static_cast<uint8_t>(primaryDensityCode.value()));
  }
  if (secondaryDensityCode.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypeSecondaryDensityCode(ctx.identity,
                                                                   mediaTypeName,
                                                                   static_cast<uint8_t>(secondaryDensityCode.value()));
  }
  if (nbWraps.has_value()) {
    std::optional<uint32_t> newNbWraps = nbWraps.value();
    ctx.catalogue.MediaType()->modifyMediaTypeNbWraps(ctx.identity, mediaTypeName, newNbWraps);
  }
  if (minlpos.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypeMinLPos(ctx.identity, mediaTypeName, minlpos);
  }
  if (maxlpos.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypeMaxLPos(ctx.identity, mediaTypeName, maxlpos);
  }
  if (comment.has_value()) {
    ctx.catalogue.MediaType()->modifyMediaTypeComment(ctx.identity, mediaTypeName, comment.value());
  }
}

void removeMediaType(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& mtn = opts.getRequired(OptionString::MEDIA_TYPE);

  ctx.catalogue.MediaType()->deleteMediaType(mtn);
}

void addPhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  common::dataStructures::PhysicalLibrary pl;
  pl.name = opts.getRequired(OptionString::PHYSICAL_LIBRARY);
  pl.manufacturer = opts.getRequired(OptionString::MANUFACTURER);
  pl.model = opts.getRequired(OptionString::LIBRARY_MODEL);
  pl.nbPhysicalCartridgeSlots = opts.getRequired(OptionUInt64::NB_PHYSICAL_CARTRIDGE_SLOTS);
  pl.nbPhysicalDriveSlots = opts.getRequired(OptionUInt64::NB_PHYSICAL_DRIVE_SLOTS);

  pl.type = opts.getOptional(OptionString::LIBRARY_TYPE);
  pl.guiUrl = opts.getOptional(OptionString::GUI_URL);
  pl.webcamUrl = opts.getOptional(OptionString::WEBCAM_URL);
  pl.location = opts.getOptional(OptionString::LIBRARY_LOCATION);
  pl.nbAvailableCartridgeSlots = opts.getOptional(OptionUInt64::NB_AVAILABLE_CARTRIDGE_SLOTS);
  pl.comment = opts.getOptional(OptionString::COMMENT);

  ctx.catalogue.PhysicalLibrary()->createPhysicalLibrary(ctx.identity, pl);
}

void changePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  common::dataStructures::UpdatePhysicalLibrary pl;

  pl.name = opts.getRequired(OptionString::PHYSICAL_LIBRARY);
  pl.model = opts.getOptional(OptionString::LIBRARY_MODEL);
  pl.type = opts.getOptional(OptionString::LIBRARY_TYPE);
  pl.guiUrl = opts.getOptional(OptionString::GUI_URL);
  pl.webcamUrl = opts.getOptional(OptionString::WEBCAM_URL);
  pl.location = opts.getOptional(OptionString::LIBRARY_LOCATION);
  pl.nbPhysicalCartridgeSlots = opts.getOptional(OptionUInt64::NB_PHYSICAL_CARTRIDGE_SLOTS);
  pl.nbAvailableCartridgeSlots = opts.getOptional(OptionUInt64::NB_AVAILABLE_CARTRIDGE_SLOTS);
  pl.nbPhysicalDriveSlots = opts.getOptional(OptionUInt64::NB_PHYSICAL_DRIVE_SLOTS);
  pl.comment = opts.getOptional(OptionString::COMMENT);
  pl.isDisabled = opts.getOptional(OptionBoolean::DISABLED);
  pl.disabledReason = opts.getOptional(OptionString::DISABLED_REASON);

  if (pl.isDisabled.has_value()) {
    if (pl.isDisabled.value() && !pl.disabledReason.has_value()) {
      throw exception::UserError(std::string("Cannot disable physical library ") + pl.name
                                 + " because the reason has not been provided");
    }
    if (!pl.isDisabled.value() && !pl.disabledReason.has_value()) {
      // when re-enabling the physical library (disabled=false), clear the previously stored disabled reason
      pl.disabledReason = std::string("");
    }
  }

  ctx.catalogue.PhysicalLibrary()->modifyPhysicalLibrary(ctx.identity, pl);
}

void removePhysicalLibrary(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  const auto& name = opts.getRequired(OptionString::PHYSICAL_LIBRARY);

  ctx.catalogue.PhysicalLibrary()->deletePhysicalLibrary(name);
}

}  // namespace cta::admin_api::commands::sync::handlers
