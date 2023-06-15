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
#include <string>

#include "catalogue/dummy/DummyVirtualOrganizationCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyVirtualOrganizationCatalogue::createVirtualOrganization(
  const common::dataStructures::SecurityIdentity& admin,
  const common::dataStructures::VirtualOrganization& vo) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::deleteVirtualOrganization(const std::string& voName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<common::dataStructures::VirtualOrganization>
  DummyVirtualOrganizationCatalogue::getVirtualOrganizations() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

common::dataStructures::VirtualOrganization
  DummyVirtualOrganizationCatalogue::getVirtualOrganizationOfTapepool(const std::string& tapepoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

common::dataStructures::VirtualOrganization
  DummyVirtualOrganizationCatalogue::getCachedVirtualOrganizationOfTapepool(const std::string& tapepoolName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& currentVoName,
  const std::string& newVoName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationReadMaxDrives(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t readMaxDrives) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationWriteMaxDrives(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t writeMaxDrives) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationMaxFileSize(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const uint64_t maxFileSize) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyVirtualOrganizationCatalogue::modifyVirtualOrganizationDiskInstanceName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& voName,
  const std::string& diskInstance) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

}  // namespace catalogue
}  // namespace cta