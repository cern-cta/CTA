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

#include "RepackRequest.hpp"


namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackRequest::RepackRequest(const std::string& address, Backend& os):
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t> (os, address) { }

//------------------------------------------------------------------------------
// RepackRequest::initialize()
//------------------------------------------------------------------------------
void RepackRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t>::initialize();
  m_payload.set_status(serializers::RepackRequestStatus::RRS_Pending);
  m_payload.set_expandmode(true);
  m_payload.set_repackmode(true);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// RepackRequest::setVid()
//------------------------------------------------------------------------------
void RepackRequest::setVid(const std::string& vid) {
  checkPayloadWritable();
  if (vid.empty()) throw exception::Exception("In RepackRequest::setVid(): empty vid");
  m_payload.set_vid(vid);
}

//------------------------------------------------------------------------------
// RepackRequest::setRepackType()
//------------------------------------------------------------------------------
void RepackRequest::setRepackType(common::dataStructures::RepackType repackType) {
  checkPayloadWritable();
  typedef common::dataStructures::RepackType RepackType;
  switch (repackType) {
  case RepackType::expandandrepack:
    // Nothing to do, this is the default case.
    break;
  case RepackType::justexpand:
    m_payload.set_repackmode(false);
    break;
  case RepackType::justrepack:
    m_payload.set_expandmode(false);
    break;
  default:
    throw exception::Exception("In RepackRequest::setRepackType(): unexpected type.");
  }
}

//------------------------------------------------------------------------------
// RepackRequest::garbageCollect()
//------------------------------------------------------------------------------
void RepackRequest::garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, 
    log::LogContext& lc, cta::catalogue::Catalogue& catalogue) {
  throw exception::Exception("In RepackRequest::garbageCollect(): not implemented.");
}


}} // namespace cta::objectstore