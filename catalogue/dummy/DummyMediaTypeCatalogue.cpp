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

#include <list>
#include <map>
#include <string>

#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/dummy/DummyMediaTypeCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyMediaTypeCatalogue::createMediaType(const common::dataStructures::SecurityIdentity& admin,
                                              const MediaType& mediaType) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::deleteMediaType(const std::string& name) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<MediaTypeWithLogs> DummyMediaTypeCatalogue::getMediaTypes() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

MediaType DummyMediaTypeCatalogue::getMediaTypeByVid(const std::string& vid) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& currentName,
                                                  const std::string& newName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity& admin,
                                                       const std::string& name,
                                                       const std::string& cartridge) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& name,
                                                             const uint64_t capacityInBytes) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& name,
                                                                const uint8_t primaryDensityCode) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                                                  const std::string& name,
                                                                  const uint8_t secondaryDensityCode) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint32_t>& nbWraps) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint64_t>& minLPos) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::optional<std::uint64_t>& maxLPos) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyMediaTypeCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& name,
                                                     const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

}  // namespace catalogue
}  // namespace cta