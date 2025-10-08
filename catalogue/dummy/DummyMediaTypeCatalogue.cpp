/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <map>
#include <string>

#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/dummy/DummyMediaTypeCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyMediaTypeCatalogue::createMediaType(const common::dataStructures::SecurityIdentity &admin,
  const MediaType &mediaType) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::deleteMediaType(const std::string &name) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<MediaTypeWithLogs> DummyMediaTypeCatalogue::getMediaTypes() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

MediaType DummyMediaTypeCatalogue::getMediaTypeByVid(const std::string & vid) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &cartridge) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeCapacityInBytes(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypePrimaryDensityCode(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeSecondaryDensityCode(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint32_t> &nbWraps) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &minLPos) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &maxLPos) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
