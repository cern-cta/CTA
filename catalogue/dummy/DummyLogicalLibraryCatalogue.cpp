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

#include "catalogue/dummy/DummyLogicalLibraryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyLogicalLibraryCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool isDisabled, const std::optional<std::string>& physicalLibraryName, const std::string &comment) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::deleteLogicalLibrary(const std::string &name) {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::LogicalLibrary> DummyLogicalLibraryCatalogue::getLogicalLibraries() const {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &physicalLibraryName) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryDisabledReason(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::setLogicalLibraryDisabled(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) {
  throw exception::NotImplementedException();
}


} // namespace cta::catalogue
