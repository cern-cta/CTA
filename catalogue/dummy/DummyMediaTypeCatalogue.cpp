/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyMediaTypeCatalogue.hpp"

#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <map>
#include <string>

namespace cta::catalogue {

void DummyMediaTypeCatalogue::createMediaType(const common::dataStructures::SecurityIdentity& admin,
                                              const MediaType& mediaType) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::deleteMediaType(const std::string& name) {
  throw exception::NotImplementedException();
}

std::vector<MediaTypeWithLogs> DummyMediaTypeCatalogue::getMediaTypes() const {
  throw exception::NotImplementedException();
}

MediaType DummyMediaTypeCatalogue::getMediaTypeByVid(const std::string& vid) const {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& currentName,
                                                  const std::string& newName) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity& admin,
                                                       const std::string& name,
                                                       const std::string& cartridge) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& name,
                                                             const uint64_t capacityInBytes) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& name,
                                                                const uint8_t primaryDensityCode) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                                                  const std::string& name,
                                                                  const uint8_t secondaryDensityCode) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint32_t>& nbWraps) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint64_t>& minLPos) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint64_t>& maxLPos) {
  throw exception::NotImplementedException();
}

void DummyMediaTypeCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::string& comment) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
