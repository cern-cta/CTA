/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyVirtualOrganizationCatalogue.hpp"

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <string>

namespace cta::catalogue {

void DummyVirtualOrganizationCatalogue::createVirtualOrganization(
  const common::dataStructures::SecurityIdentity& admin,
  const common::dataStructures::VirtualOrganization& vo) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::deleteVirtualOrganization(const std::string& voName) {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::VirtualOrganization>
DummyVirtualOrganizationCatalogue::getVirtualOrganizations() const {
  throw exception::NotImplementedException();
}

common::dataStructures::VirtualOrganization
DummyVirtualOrganizationCatalogue::getVirtualOrganizationOfTapepool(const std::string& tapepoolName) const {
  throw exception::NotImplementedException();
}

common::dataStructures::VirtualOrganization
DummyVirtualOrganizationCatalogue::getCachedVirtualOrganizationOfTapepool(const std::string& tapepoolName) const {
  throw exception::NotImplementedException();
}

std::optional<common::dataStructures::VirtualOrganization>
DummyVirtualOrganizationCatalogue::getDefaultVirtualOrganizationForRepack() const {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& currentVoName,
  const std::string& newVoName) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationReadMaxDrives(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t readMaxDrives) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationWriteMaxDrives(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t writeMaxDrives) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationMaxFileSize(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t maxFileSize) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationDiskInstanceName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const std::string& diskInstance) {
  throw exception::NotImplementedException();
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationIsRepackVo(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const bool isRepackVo) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
