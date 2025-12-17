/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

std::list<MediaTypeWithLogs> DummyMediaTypeCatalogue::getMediaTypes() const {
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
