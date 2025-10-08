/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <map>
#include <string>

#include "catalogue/dummy/DummyDiskSystemCatalogue.hpp"
#include "catalogue/dummy/DummyDiskInstanceSpaceCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "disk/DiskSystem.hpp"

namespace cta::catalogue {

void DummyDiskSystemCatalogue::createDiskSystem(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName,
  const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime,const std::string &comment) {
  m_diskSystemList.push_back({name, DummyDiskInstanceSpaceCatalogue::m_diskInstanceSpaces.at(diskInstanceSpaceName),
    fileRegexp, targetedFreeSpace, sleepTime, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{},
    comment});
}

void DummyDiskSystemCatalogue::deleteDiskSystem(const std::string &name) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

disk::DiskSystemList DummyDiskSystemCatalogue::getAllDiskSystems() const {
  return m_diskSystemList;
}

void DummyDiskSystemCatalogue::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &fileRegexp) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskSystemCatalogue::modifyDiskSystemTargetedFreeSpace(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const uint64_t targetedFreeSpace) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskSystemCatalogue::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskSystemCatalogue::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
  const std::string& name, const uint64_t sleepTime) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskSystemCatalogue::modifyDiskSystemDiskInstanceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyDiskSystemCatalogue::modifyDiskSystemDiskInstanceSpaceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const std::string &diskInstanceSpaceName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

bool DummyDiskSystemCatalogue::diskSystemExists(const std::string &name) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
