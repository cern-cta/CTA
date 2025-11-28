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

#include "catalogue/dummy/DummyStorageClassCatalogue.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyStorageClassCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::StorageClass &storageClass) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::deleteStorageClass(const std::string &storageClassName) {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::StorageClass> DummyStorageClassCatalogue::getStorageClasses() const {
  throw exception::NotImplementedException();
}

common::dataStructures::StorageClass DummyStorageClassCatalogue::getStorageClass(const std::string &name) const {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbCopies) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  throw exception::NotImplementedException();
}

} // namespace cta::catalogue
