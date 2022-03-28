/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "DriveRegister.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "GenericObject.hpp"
#include "RootEntry.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/MountType.hpp"
#include <set>
#include <google/protobuf/util/json_util.h>
#include <iostream>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// DriveRegister::DriveRegister())
//------------------------------------------------------------------------------
DriveRegister::DriveRegister(const std::string & address, Backend & os):
  ObjectOps<serializers::DriveRegister, serializers::DriveRegister_t>(os, address) { }

//------------------------------------------------------------------------------
// DriveRegister::DriveRegister())
//------------------------------------------------------------------------------
DriveRegister::DriveRegister(GenericObject& go):
  ObjectOps<serializers::DriveRegister, serializers::DriveRegister_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

//------------------------------------------------------------------------------
// DriveRegister::dump())
//------------------------------------------------------------------------------
std::string DriveRegister::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

//------------------------------------------------------------------------------
// DriveRegister::initialize())
//------------------------------------------------------------------------------
void DriveRegister::initialize() {
  // Setup underlying object
  ObjectOps<serializers::DriveRegister, serializers::DriveRegister_t>::initialize();
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// DriveRegister::garbageCollect())
//------------------------------------------------------------------------------
void DriveRegister::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
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
  log::ScopedParamContainer params(lc);
  params.add("driveRegisterObject", getAddressIfSet());
  lc.log(log::INFO, "In DriveRegister::garbageCollect(): Garbage collected and removed drive register object.");
}

//------------------------------------------------------------------------------
// DriveRegister::getDriveAddresses()
//------------------------------------------------------------------------------
std::list<DriveRegister::DriveAddress> DriveRegister::getDriveAddresses() {
  checkPayloadReadable();
  std::list<DriveAddress> ret;
  for (auto & d: m_payload.drives()) {
    ret.push_back(DriveAddress());
    ret.back().driveName=d.drivename();
    ret.back().driveStateAddress=d.drivestateaddress();
  }
  return ret;
}

//------------------------------------------------------------------------------
// DriveRegister::getDriveAddresse()
//------------------------------------------------------------------------------
std::string DriveRegister::getDriveAddress(const std::string& driveName) {
  checkPayloadReadable();
  for (auto & d:m_payload.drives())
    if (d.drivename() == driveName)
      return d.drivestateaddress();
  throw NoSuchDrive("In DriveRegister::getDriveAddresse(): no such drive.");
}

//------------------------------------------------------------------------------
// DriveRegister::getDriveAddresse()
//------------------------------------------------------------------------------
void DriveRegister::setDriveAddress(const std::string& driveName, const std::string& driveAddress) {
  checkPayloadWritable();
  for (int i=0; i< m_payload.mutable_drives()->size(); i++) {
    auto d=m_payload.mutable_drives(i);
    if (d->drivename() == driveName) {
      d->set_drivestateaddress(driveAddress);
      return;
    }
  }
  auto d=m_payload.mutable_drives()->Add();
  d->set_drivename(driveName);
  d->set_drivestateaddress(driveAddress);
  return;
}

//------------------------------------------------------------------------------
// DriveRegister::removeDrive())
//------------------------------------------------------------------------------
void DriveRegister::removeDrive(const std::string  & driveName) {
  checkPayloadWritable();
  auto driveToRemove = m_payload.mutable_drives()->begin();
  while (driveToRemove != m_payload.mutable_drives()->end()) {
    if ( driveToRemove->drivename() == driveName) {
      m_payload.mutable_drives()->erase(driveToRemove);
      return;
    }
    driveToRemove++;
  }
  std::stringstream err;
  err << "In DriveRegister::removeDrive(): drive not found: " << driveName;
  throw cta::exception::Exception(err.str()); 
}

//------------------------------------------------------------------------------
// DriveRegister::isEmpty())
//------------------------------------------------------------------------------
bool DriveRegister::isEmpty() {
  checkPayloadReadable();
  if (m_payload.drives_size())
    return false;
  return true;
}

}} // namespace cta::objectstore
