/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyLogicalLibraryCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyLogicalLibraryCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                                        const std::string& name,
                                                        const bool isDisabled,
                                                        const std::optional<std::string>& physicalLibraryName,
                                                        const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::deleteLogicalLibrary(const std::string& name) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::LogicalLibrary> DummyLogicalLibraryCatalogue::getLogicalLibraries() const {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& currentName,
                                                            const std::string& newName) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin,
                                                               const std::string& name,
                                                               const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryPhysicalLibrary(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& physicalLibraryName) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryDisabledReason(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& disabledReason) {
  throw exception::NotImplementedException();
}

void DummyLogicalLibraryCatalogue::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& name,
                                                             const bool disabledValue) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
