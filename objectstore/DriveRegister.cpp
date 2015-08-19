/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "DriveRegister.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "GenericObject.hpp"
#include "RootEntry.hpp"

cta::objectstore::DriveRegister::DriveRegister(const std::string & address, Backend & os):
  ObjectOps<serializers::DriveRegister>(os, address) { }

cta::objectstore::DriveRegister::DriveRegister(GenericObject& go):
  ObjectOps<serializers::DriveRegister>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::DriveRegister::initialize() {
  // Setup underlying object
  ObjectOps<serializers::DriveRegister>::initialize();
  m_payloadInterpreted = true;
}

void cta::objectstore::DriveRegister::garbageCollect(const std::string &presumedOwner) {
  checkPayloadWritable();
  // If the agent is not anymore the owner of the object, then only the very
  // last operation of the drive register creation failed. We have nothing to do.
  if (presumedOwner != m_header.owner())
    return;
  // If the owner is still the agent, we have 2 possibilities:
  // 1) The register is referenced by the root entry. We just need to officialise
  // the ownership on the register (if not, we will either get the NotAllocated 
  // exception) or the address will be different.
  {
    RootEntry re(m_objectStore);
    ScopedSharedLock rel (re);
    re.fetch();
    try {
      if (re.getAgentRegisterAddress() == getAddressIfSet()) {
        setOwner(re.getAddressIfSet());
        commit();
        return;
      }
    } catch (RootEntry::NotAllocated &) {}
  }
  // 2) The tape pool is not referenced in the root entry. We can just clean it up.
  if (!isEmpty()) {
    throw (NotEmpty("Trying to garbage collect a non-empty AgentRegister: internal error"));
  }
  remove();
}

// This operator will be used in the following usage of the templated
// findElement and removeOccurences
namespace {
  bool operator==(const std::string & driveName, 
    const cta::objectstore::serializers::DriveInfo & di) {
    return di.drivename() == driveName;
  }
}

void cta::objectstore::DriveRegister::addDrive(const std::string & driveName,
    const std::string & logicalLibrary) { 
  //add logical library to the parameters
  checkPayloadWritable();
  // Check that we are not trying to duplicate a drive
  try {
    serializers::findElement(m_payload.drives(), driveName);
    throw DuplicateEntry("In DriveRegister::addDrive: entry already exists");
  } catch (serializers::NotFound &) {}
  serializers::DriveInfo * driveInfo = m_payload.add_drives();
  driveInfo->set_drivename(driveName);
  driveInfo->set_logicallibrary(logicalLibrary);
  driveInfo->set_lastupdatetime(0);
  driveInfo->set_currentmount(serializers::MountType::NoMount_t);
  driveInfo->set_mounttime(0);
  driveInfo->set_currentvid("");
}

auto cta::objectstore::DriveRegister::dumpDrives() -> std::list<DriveInfo> {
  checkPayloadReadable();
  auto & dl = m_payload.drives();
  std::list<DriveInfo> ret;
  for (auto d=dl.begin(); d!=dl.end(); d++) {
    ret.push_back(DriveInfo());
    auto & di = ret.back();
    di.currentVid = d->currentvid();
    di.currentTapePool = d->currenttapepool();
    di.lastUpdateTime = d->lastupdatetime();
    di.logicalLibrary = d->logicallibrary();
    switch (d->currentmount()) {
      case serializers::MountType::Archive_t:
        di.mountStatus = MountStatus::ArchiveMount;
        break;
      case serializers::MountType::Retrieve_t:
        di.mountStatus = MountStatus::RetrieveMount;
        break;
      default:
        di.mountStatus = MountStatus::NoMount;
    }
    di.mountTime = d->mounttime();
    di.name = d->drivename();
  }
  return ret;
}


bool cta::objectstore::DriveRegister::isEmpty() {
  checkPayloadReadable();
  if (m_payload.drives_size())
    return false;
  return true;
}




