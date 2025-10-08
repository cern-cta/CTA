/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <string>

#include "catalogue/dummy/DummyVirtualOrganizationCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyVirtualOrganizationCatalogue::createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::VirtualOrganization &vo) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::deleteVirtualOrganization(const std::string &voName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<common::dataStructures::VirtualOrganization> DummyVirtualOrganizationCatalogue::getVirtualOrganizations() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

common::dataStructures::VirtualOrganization DummyVirtualOrganizationCatalogue::getVirtualOrganizationOfTapepool(
  const std::string &tapepoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

common::dataStructures::VirtualOrganization DummyVirtualOrganizationCatalogue::getCachedVirtualOrganizationOfTapepool(
  const std::string & tapepoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::optional<common::dataStructures::VirtualOrganization> DummyVirtualOrganizationCatalogue::getDefaultVirtualOrganizationForRepack() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName,
  const std::string &newVoName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationReadMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationWriteMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationMaxFileSize(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationDiskInstanceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationIsRepackVo(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const bool isRepackVo) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

} // namespace cta::catalogue
