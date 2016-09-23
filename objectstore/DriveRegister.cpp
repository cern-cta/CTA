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
#include <json-c/json.h>
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
  // TODO: update following structure changes.
  checkPayloadReadable();
  std::stringstream ret;
  ret << "DriveRegister" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object * array = json_object_new_array();
  for (auto i = m_payload.drives().begin(); i!=m_payload.drives().end(); i++) {
    json_object * jot = json_object_new_object();
    
    json_object_object_add(jot, "drivename", json_object_new_string(i->drivename().c_str()));
    json_object_object_add(jot, "logicallibrary", json_object_new_string(i->logicallibrary().c_str()));
    json_object_object_add(jot, "sessionid", json_object_new_int64(i->sessionid()));
    json_object_object_add(jot, "bytestransferedinsession", json_object_new_int64(i->bytestransferedinsession()));
    json_object_object_add(jot, "filestransferedinsession", json_object_new_int64(i->filestransferedinsession()));
    json_object_object_add(jot, "latestbandwidth", json_object_new_double(i->latestbandwidth()));
    json_object_object_add(jot, "sessionstarttime", json_object_new_int64(i->sessionstarttime()));
    json_object_object_add(jot, "mountstarttime", json_object_new_int64(i->mountstarttime()));
    json_object_object_add(jot, "transferstarttime", json_object_new_int64(i->transferstarttime()));
    json_object_object_add(jot, "unloadstarttime", json_object_new_int64(i->unloadstarttime()));
    json_object_object_add(jot, "unmountstarttime", json_object_new_int64(i->unmountstarttime()));
    json_object_object_add(jot, "drainingstarttime", json_object_new_int64(i->drainingstarttime()));
    json_object_object_add(jot, "downorupstarttime", json_object_new_int64(i->downorupstarttime()));
    json_object_object_add(jot, "cleanupstarttime", json_object_new_int64(i->cleanupstarttime()));
    json_object_object_add(jot, "lastupdatetime", json_object_new_int64(i->lastupdatetime()));
    json_object_object_add(jot, "currentvid", json_object_new_string(i->currentvid().c_str()));
    json_object_object_add(jot, "currenttapepool", json_object_new_string(i->currenttapepool().c_str()));
    
// TODO: Creation log should be fully supported or dropped.
//    json_object * creationlog_jot = json_object_new_object();
//    json_object_object_add(creationlog_jot, "host", json_object_new_string(i->creationlog().host().c_str()));
//    json_object_object_add(creationlog_jot, "time", json_object_new_int64(i->creationlog().time()));
//    json_object_object_add(jot, "creationlog", creationlog_jot);
    
    json_object * mounttype_jot = json_object_new_object();
    json_object_object_add(mounttype_jot, "type", json_object_new_int(i->mounttype()));
    json_object_object_add(jot, "mounttype", mounttype_jot);
    
    json_object * drivestatus_jot = json_object_new_object();
    json_object_object_add(drivestatus_jot, "status", json_object_new_int(i->drivestatus()));
    json_object_object_add(jot, "drivestatus", drivestatus_jot);
  
    json_object_array_add(array, jot);
  }
  json_object_object_add(jo, "drives", array);
  
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  return ret.str();
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
void DriveRegister::garbageCollect(const std::string &presumedOwner) {
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
    ret.back().bytesTransferedInSession    = d.bytestransferedinsession();
    ret.back().filesTransferedInSession    = d.filestransferedinsession();
    ret.back().latestBandwidth             = d.latestbandwidth();
    ret.back().sessionStartTime            = d.sessionstarttime();
    ret.back().mountStartTime              = d.mountstarttime();
    ret.back().transferStartTime           = d.transferstarttime();
    ret.back().unloadStartTime             = d.unloadstarttime();
    ret.back().unmountStartTime            = d.unmountstarttime();
    ret.back().drainingStartTime           = d.drainingstarttime();
    ret.back().downOrUpStartTime           = d.downorupstarttime();
    ret.back().cleanupStartTime            = d.cleanupstarttime();
    ret.back().lastUpdateTime              = d.lastupdatetime();
    ret.back().startStartTime              = d.startstarttime();
    ret.back().mountType                   = (common::dataStructures::MountType) d.mounttype();
    ret.back().driveStatus                 = (common::dataStructures::DriveStatus) d.drivestatus();
    ret.back().desiredDriveState.up        = d.desiredup();
    ret.back().desiredDriveState.forceDown = d.desiredforcedown();
    ret.back().currentVid                  = d.currentvid();
    ret.back().currentTapePool             = d.currenttapepool();
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
      ret.bytesTransferedInSession    = d.bytestransferedinsession();
      ret.filesTransferedInSession    = d.filestransferedinsession();
      ret.latestBandwidth             = d.latestbandwidth();
      ret.sessionStartTime            = d.sessionstarttime();
      ret.mountStartTime              = d.mountstarttime();
      ret.transferStartTime           = d.transferstarttime();
      ret.unloadStartTime             = d.unloadstarttime();
      ret.unmountStartTime            = d.unmountstarttime();
      ret.drainingStartTime           = d.drainingstarttime();
      ret.downOrUpStartTime           = d.downorupstarttime();
      ret.cleanupStartTime            = d.cleanupstarttime();
      ret.lastUpdateTime              = d.lastupdatetime();
      ret.startStartTime              = d.startstarttime();
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
  ds->set_bytestransferedinsession(driveState.bytesTransferedInSession);
  ds->set_filestransferedinsession(driveState.filesTransferedInSession);
  ds->set_latestbandwidth(driveState.latestBandwidth);
  ds->set_sessionstarttime(driveState.sessionStartTime);
  ds->set_mountstarttime(driveState.mountStartTime);
  ds->set_transferstarttime(driveState.transferStartTime);
  ds->set_unloadstarttime(driveState.unloadStartTime);
  ds->set_unmountstarttime(driveState.unmountStartTime);
  ds->set_drainingstarttime(driveState.drainingStartTime);
  ds->set_downorupstarttime(driveState.downOrUpStartTime);
  ds->set_cleanupstarttime(driveState.cleanupStartTime);
  ds->set_lastupdatetime(driveState.lastUpdateTime);
  ds->set_startstarttime(driveState.startStartTime);
  ds->set_mounttype((uint32_t)driveState.mountType);
  ds->set_drivestatus((uint32_t)driveState.driveStatus);
  ds->set_desiredup(driveState.desiredDriveState.up);
  ds->set_desiredforcedown(driveState.desiredDriveState.forceDown);
  ds->set_currentvid(driveState.currentVid);
  ds->set_currenttapepool(driveState.currentTapePool);
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
