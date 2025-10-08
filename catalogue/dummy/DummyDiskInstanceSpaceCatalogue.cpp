/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <map>
#include <string>

#include "catalogue/dummy/DummyDiskInstanceSpaceCatalogue.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

std::map<std::string, common::dataStructures::DiskInstanceSpace, std::less<>> DummyDiskInstanceSpaceCatalogue::m_diskInstanceSpaces;

void DummyDiskInstanceSpaceCatalogue::deleteDiskInstanceSpace(const std::string &name,
  const std::string &diskInstance) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskInstanceSpaceCatalogue::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL,
  const uint64_t refreshInterval, const std::string &comment) {
    m_diskInstanceSpaces[name] = {name, diskInstance, freeSpaceQueryURL, refreshInterval, 0, 0, comment,
      common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
}

std::list<common::dataStructures::DiskInstanceSpace> DummyDiskInstanceSpaceCatalogue::getAllDiskInstanceSpaces() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceRefreshInterval(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const uint64_t refreshInterval) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceFreeSpace(const std::string &name,
  const std::string &diskInstance, const uint64_t freeSpace) {
  m_diskInstanceSpaces[name].freeSpace = freeSpace;
  m_diskInstanceSpaces[name].lastRefreshTime = time(nullptr);
}

void DummyDiskInstanceSpaceCatalogue::modifyDiskInstanceSpaceQueryURL(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance,
  const std::string &freeSpaceQueryURL) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
