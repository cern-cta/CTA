/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyLogicalLibraryCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyLogicalLibraryCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool isDisabled, const std::optional<std::string>& physicalLibraryName, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::deleteLogicalLibrary(const std::string &name) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::LogicalLibrary> DummyLogicalLibraryCatalogue::getLogicalLibraries() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &physicalLibraryName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::modifyLogicalLibraryDisabledReason(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyLogicalLibraryCatalogue::setLogicalLibraryDisabled(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}


} // namespace cta::catalogue
