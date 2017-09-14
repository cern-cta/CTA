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
}

//------------------------------------------------------------------------------
// DriveRegister::getAllDrivesState())
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> DriveRegister::getAllDrivesState() {
  using cta::common::dataStructures::DriveState;
  // Get all drives states
  checkPayloadReadable();
  std::list<cta::common::dataStructures::DriveState> ret;
  for (auto & d: m_payload.drives()) {
    ret.push_back(DriveState());
    ret.back().driveName                   = d.drivename();
    ret.back().host                        = d.host();
    ret.back().logicalLibrary              = d.logicallibrary();
    ret.back().sessionId                   = d.sessionid();
    ret.back().bytesTransferredInSession    = d.bytestransferedinsession();
    ret.back().filesTransferredInSession    = d.filestransferedinsession();
    ret.back().latestBandwidth             = d.latestbandwidth();
    ret.back().sessionStartTime            = d.sessionstarttime();
    ret.back().mountStartTime              = d.mountstarttime();
    ret.back().transferStartTime           = d.transferstarttime();
    ret.back().unloadStartTime             = d.unloadstarttime();
    ret.back().unmountStartTime            = d.unmountstarttime();
    ret.back().drainingStartTime           = d.drainingstarttime();
    ret.back().downOrUpStartTime           = d.downorupstarttime();
    ret.back().probeStartTime              = d.probestarttime();
    ret.back().cleanupStartTime            = d.cleanupstarttime();
    ret.back().lastUpdateTime              = d.lastupdatetime();
    ret.back().startStartTime              = d.startstarttime();
    ret.back().shutdownTime                = d.shutdowntime();
    ret.back().mountType                   = (common::dataStructures::MountType) d.mounttype();
    ret.back().driveStatus                 = (common::dataStructures::DriveStatus) d.drivestatus();
    ret.back().desiredDriveState.up        = d.desiredup();
    ret.back().desiredDriveState.forceDown = d.desiredforcedown();
    ret.back().currentVid                  = d.currentvid();
    ret.back().currentTapePool             = d.currenttapepool();
    if (d.has_nextmounttype()) { ret.back().nextMountType  = (common::dataStructures::MountType) d.nextmounttype(); }
    if (d.has_nexttapepool()) { ret.back().nextTapepool  = d.nexttapepool(); }
    if (d.has_nextvid()) { ret.back().nextVid  = d.nextvid(); }   
  }
  return ret;
}

//------------------------------------------------------------------------------
// DriveRegister::getDriveState())
//------------------------------------------------------------------------------
cta::common::dataStructures::DriveState DriveRegister::getDriveState(const std::string& driveName) {
  using cta::common::dataStructures::DriveState;
  // Find the drive for which we want the status
  checkPayloadReadable();
  for (auto & d: m_payload.drives()) {
    if (d.drivename() == driveName) {
      cta::common::dataStructures::DriveState ret;
      ret.driveName                   = d.drivename();
      ret.host                        = d.host();
      ret.logicalLibrary              = d.logicallibrary();
      ret.sessionId                   = d.sessionid();
      ret.bytesTransferredInSession    = d.bytestransferedinsession();
      ret.filesTransferredInSession    = d.filestransferedinsession();
      ret.latestBandwidth             = d.latestbandwidth();
      ret.sessionStartTime            = d.sessionstarttime();
      ret.mountStartTime              = d.mountstarttime();
      ret.transferStartTime           = d.transferstarttime();
      ret.unloadStartTime             = d.unloadstarttime();
      ret.unmountStartTime            = d.unmountstarttime();
      ret.drainingStartTime           = d.drainingstarttime();
      ret.downOrUpStartTime           = d.downorupstarttime();
      ret.probeStartTime              = d.probestarttime();
      ret.cleanupStartTime            = d.cleanupstarttime();
      ret.lastUpdateTime              = d.lastupdatetime();
      ret.startStartTime              = d.startstarttime();
      ret.shutdownTime                = d.shutdowntime();
      ret.mountType                   = (common::dataStructures::MountType) d.mounttype();
      ret.driveStatus                 = (common::dataStructures::DriveStatus) d.drivestatus();
      ret.desiredDriveState.up        = d.desiredup();
      ret.desiredDriveState.forceDown = d.desiredforcedown();
      ret.currentVid                  = d.currentvid();
      ret.currentTapePool             = d.currenttapepool();
      return ret;
    }
  }
  throw cta::exception::Exception("In DriveRegister::getDriveState(): no such drive.");
}

//------------------------------------------------------------------------------
// DriveRegister::setDriveState())
//------------------------------------------------------------------------------
void DriveRegister::setDriveState(const cta::common::dataStructures::DriveState driveState) {
  using cta::common::dataStructures::DriveState;
  checkPayloadWritable();
  // Find the drive to update (new or existing)
  serializers::DriveState * ds = nullptr;
  for (ssize_t i=0; i<m_payload.mutable_drives()->size(); i++) {
    if (m_payload.mutable_drives(i)->drivename() == driveState.driveName) {
      ds = m_payload.mutable_drives(i);
      goto update;
    }
  }
  // The drive was not found. We will create it.
  ds = m_payload.mutable_drives()->Add();
  ds->set_drivename(driveState.driveName);
update:
  ds->set_host(driveState.host);
  ds->set_logicallibrary(driveState.logicalLibrary);
  ds->set_sessionid(driveState.sessionId);
  ds->set_bytestransferedinsession(driveState.bytesTransferredInSession);
  ds->set_filestransferedinsession(driveState.filesTransferredInSession);
  ds->set_latestbandwidth(driveState.latestBandwidth);
  ds->set_sessionstarttime(driveState.sessionStartTime);
  ds->set_mountstarttime(driveState.mountStartTime);
  ds->set_transferstarttime(driveState.transferStartTime);
  ds->set_unloadstarttime(driveState.unloadStartTime);
  ds->set_unmountstarttime(driveState.unmountStartTime);
  ds->set_drainingstarttime(driveState.drainingStartTime);
  ds->set_downorupstarttime(driveState.downOrUpStartTime);
  ds->set_probestarttime(driveState.probeStartTime);
  ds->set_cleanupstarttime(driveState.cleanupStartTime);
  ds->set_lastupdatetime(driveState.lastUpdateTime);
  ds->set_startstarttime(driveState.startStartTime);
  ds->set_shutdowntime(driveState.shutdownTime);
  ds->set_mounttype((uint32_t)driveState.mountType);
  ds->set_drivestatus((uint32_t)driveState.driveStatus);
  ds->set_desiredup(driveState.desiredDriveState.up);
  ds->set_desiredforcedown(driveState.desiredDriveState.forceDown);
  ds->set_currentvid(driveState.currentVid);
  ds->set_currenttapepool(driveState.currentTapePool);
}


//------------------------------------------------------------------------------
// DriveRegister::setNextDriveState())
//------------------------------------------------------------------------------
void DriveRegister::setNextDriveState(const cta::common::dataStructures::DriveNextState driveNextState) {
  using cta::common::dataStructures::MountType;
  checkPayloadWritable();
  // Find the drive to update (new or existing)
  serializers::DriveState * ds = nullptr;
  for (ssize_t i=0; i<m_payload.mutable_drives()->size(); i++) {
    if (m_payload.mutable_drives(i)->drivename() == driveNextState.driveName) {
      ds = m_payload.mutable_drives(i);
      goto update;
    }
  }
  // The drive was not found. We will create it.
  ds = m_payload.mutable_drives()->Add();
  ds->set_drivename(driveNextState.driveName);
update: 
  switch(driveNextState.mountType) {
  case MountType::Archive:
    ds->set_nexttapepool(driveNextState.tapepool);
    // No break on purpose.
  case MountType::Label:
  case MountType::Retrieve:
    ds->set_nextmounttype((int)driveNextState.mountType);
    ds->set_nextvid(driveNextState.vid);
    break;
  default:
  {
    std::stringstream err;
    err << "In DriveRegister::setNextDriveState(): unknown mount type: " 
        << cta::common::dataStructures::toString(driveNextState.mountType);
    throw cta::exception::Exception(err.str());
  }
  }
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
