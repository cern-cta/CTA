/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyDiskInstanceCatalogue.hpp"

#include "common/dataStructures/DiskInstance.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <list>
#include <map>
#include <string>

namespace cta::catalogue {

void DummyDiskInstanceCatalogue::createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                                                    const std::string& name,
                                                    const std::string& comment) {
  m_diskInstances[name] = {name, comment, common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
}

void DummyDiskInstanceCatalogue::deleteDiskInstance(const std::string& name) {
  throw exception::NotImplementedException();
}

void DummyDiskInstanceCatalogue::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                                           const std::string& name,
                                                           const std::string& comment) {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::DiskInstance> DummyDiskInstanceCatalogue::getAllDiskInstances() const {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
