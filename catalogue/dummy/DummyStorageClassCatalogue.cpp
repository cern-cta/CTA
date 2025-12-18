/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyStorageClassCatalogue.hpp"

#include "common/dataStructures/StorageClass.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyStorageClassCatalogue::createStorageClass(const common::dataStructures::SecurityIdentity& admin,
                                                    const common::dataStructures::StorageClass& storageClass) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::deleteStorageClass(const std::string& storageClassName) {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::StorageClass> DummyStorageClassCatalogue::getStorageClasses() const {
  throw exception::NotImplementedException();
}

common::dataStructures::StorageClass DummyStorageClassCatalogue::getStorageClass(const std::string& name) const {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& name,
                                                            const uint64_t nbCopies) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin,
                                                           const std::string& name,
                                                           const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassVo(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& name,
                                                      const std::string& vo) {
  throw exception::NotImplementedException();
}

void DummyStorageClassCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin,
                                                        const std::string& currentName,
                                                        const std::string& newName) {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
