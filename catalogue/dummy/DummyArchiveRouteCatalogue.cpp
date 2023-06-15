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

#include "catalogue/dummy/DummyArchiveRouteCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyArchiveRouteCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity& admin,
                                                    const std::string& storageClassName,
                                                    const uint32_t copyNb,
                                                    const std::string& tapePoolName,
                                                    const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyArchiveRouteCatalogue::deleteArchiveRoute(const std::string& storageClassName, const uint32_t copyNb) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<common::dataStructures::ArchiveRoute> DummyArchiveRouteCatalogue::getArchiveRoutes() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<common::dataStructures::ArchiveRoute>
  DummyArchiveRouteCatalogue::getArchiveRoutes(const std::string& storageClassName,
                                               const std::string& tapePoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                                                const std::string& storageClassName,
                                                                const uint32_t copyNb,
                                                                const std::string& tapePoolName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyArchiveRouteCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin,
                                                           const std::string& storageClassName,
                                                           const uint32_t copyNb,
                                                           const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

}  // namespace catalogue
}  // namespace cta