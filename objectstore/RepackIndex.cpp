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

#include "RepackIndex.hpp"
#include "GenericObject.hpp"
#include <google/protobuf/util/json_util.h>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// RepackIndex::DriveRegister()
//------------------------------------------------------------------------------
RepackIndex::RepackIndex(const std::string& address, Backend& os):
  ObjectOps<serializers::RepackIndex, serializers::RepackIndex_t>(os, address) { }

//------------------------------------------------------------------------------
// RepackIndex::DriveRegister()
//------------------------------------------------------------------------------
RepackIndex::RepackIndex(GenericObject& go):
  ObjectOps<serializers::RepackIndex, serializers::RepackIndex_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

//------------------------------------------------------------------------------
// RepackIndex::dump()
//------------------------------------------------------------------------------
std::string RepackIndex::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

//------------------------------------------------------------------------------
// RepackIndex::initialize()
//------------------------------------------------------------------------------
void RepackIndex::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RepackIndex, serializers::RepackIndex_t>::initialize();
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// RepackIndex::garbageCollect()
//------------------------------------------------------------------------------
void RepackIndex::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  // We should never have to garbage collect
  log::ScopedParamContainer params(lc);
  params.add("repackTapeRegister", getAddressIfSet())
        .add("currentOwner", getOwner())
        .add("backupOwner", getBackupOwner())
        .add("presumedOwner", presumedOwner);
  lc.log(log::ERR, "In RepackIndex::garbageCollect(): Repack Tape Register should not require garbage collection.");
  throw exception::Exception("In RepackIndex::garbageCollect(): Repack Tape Register should not require garbage collection");
}

//------------------------------------------------------------------------------
// RepackIndex::getRepackTapeAddress()
//------------------------------------------------------------------------------
std::string RepackIndex::getRepackRequestAddress(const std::string& vid) {
  checkPayloadReadable();
  for (auto & rt: m_payload.repackrequestpointers()) {
    if (rt.vid() == vid)
      return rt.address();
  }
  throw NoSuchVID("In RepackIndex::getRepackTapeAddress(): no such VID");
}


//------------------------------------------------------------------------------
// RepackIndex::getRepackTapeAddresses()
//------------------------------------------------------------------------------
std::list<RepackIndex::RepackRequestAddress> RepackIndex::getRepackRequestsAddresses() {
  checkHeaderReadable();
  std::list<RepackRequestAddress> ret;
  for (auto &rt: m_payload.repackrequestpointers()) {
    ret.push_back(RepackRequestAddress());
    ret.back().driveStateAddress = rt.address();
    ret.back().vid = rt.vid();
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackIndex::isEmpty()
//------------------------------------------------------------------------------
bool RepackIndex::isEmpty() {
  checkPayloadReadable();
  return m_payload.repackrequestpointers().empty();
}

//------------------------------------------------------------------------------
// DriveRegister::getDriveAddresse()
//------------------------------------------------------------------------------
void RepackIndex::removeRepackRequest(const std::string& vid) {
  checkPayloadWritable();
  bool found = false;
  auto vidToRemove = m_payload.mutable_repackrequestpointers()->begin();
  while (vidToRemove != m_payload.mutable_repackrequestpointers()->end()) {
    if ( vidToRemove->vid() == vid) {
      vidToRemove = m_payload.mutable_repackrequestpointers()->erase(vidToRemove);
      found = true;
    } else {
      vidToRemove++;
    }
  }
  if (!found) {
    std::stringstream err;
    err << "In RepackIndex::removeRepackTape(): vid not found: " << vid;
    throw cta::exception::Exception(err.str()); 
  }
}

//------------------------------------------------------------------------------
// DriveRegister::setRepackTapeAddress()
//------------------------------------------------------------------------------
void RepackIndex::addRepackRequestAddress(const std::string& vid, 
    const std::string& repackTapeAddress) {
  checkPayloadWritable();
  for (int i=0; i< m_payload.mutable_repackrequestpointers()->size(); i++) {
    auto rt=m_payload.mutable_repackrequestpointers(i);
    if (rt->vid() == vid) {
      throw VidAlreadyRegistered("In RepackIndex::addRepackRequestAddress(): VID already has a repack request.");
    }
  }
  auto rt=m_payload.mutable_repackrequestpointers()->Add();
  rt->set_vid(vid);
  rt->set_address(repackTapeAddress);
  return;
}

}} // namespace cta::objectstore
