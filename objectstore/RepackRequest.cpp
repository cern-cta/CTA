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

//------------------------------------------------------------------------------
// RepackRequest::asyncUpdateOwner()
//------------------------------------------------------------------------------
RepackRequest::AsyncOwnerUpdater* RepackRequest::asyncUpdateOwner(const std::string& owner, const std::string& previousOwner, 
    cta::optional<serializers::RepackRequestStatus> newStatus) {
  std::unique_ptr<AsyncOwnerUpdater> ret(new AsyncOwnerUpdater);
  auto & retRef = *ret;
  ret->m_updaterCallback=
    [this, owner, previousOwner, &retRef, newStatus](const std::string &in)->std::string {
      // We have a locked and fetched object, so we just need to work on its representation.
      retRef.m_timingReport.insertAndReset("lockFetchTime", retRef.m_timer);
      serializers::ObjectHeader oh;
      if (!oh.ParseFromString(in)) {
        // Use a the tolerant parser to assess the situation.
        oh.ParsePartialFromString(in);
        throw cta::exception::Exception(std::string("In RepackRequest::asyncUpdateOwner(): could not parse header: ")+
          oh.InitializationErrorString());
      }
      if (oh.type() != serializers::ObjectType::RepackRequest_t) {
        std::stringstream err;
        err << "In RepackRequest::asyncUpdateOwner()::lambda(): wrong object type: " << oh.type();
        throw cta::exception::Exception(err.str());
      }
      // Change the owner if conditions are met.
      if (oh.owner() != previousOwner) {
        throw Backend::WrongPreviousOwner("In RepackRequest::asyncUpdateOwner()::lambda(): Request not owned.");
      }
      oh.set_owner(owner);
      // We only need to modify the pay load if there is a status change.
      if (newStatus) {
        serializers::RepackRequest payload;
        if (!payload.ParseFromString(oh.payload())) {
          // Use a the tolerant parser to assess the situation.
          payload.ParsePartialFromString(oh.payload());
          throw cta::exception::Exception(std::string("In RepackRequest::asyncUpdateOwner(): could not parse payload: ")+
            payload.InitializationErrorString());
        }
        payload.set_status(newStatus.value());
        oh.set_payload(payload.SerializePartialAsString());
      }
      return oh.SerializeAsString();
    };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(), ret->m_updaterCallback));
  return ret.release();  
}

//------------------------------------------------------------------------------
// RepackRequest::AsyncOwnerUpdater::wait()
//------------------------------------------------------------------------------
void RepackRequest::AsyncOwnerUpdater::wait() {
  m_backendUpdater->wait();
  m_timingReport.insertAndReset("commitUnlockTime", m_timer);
}




}} // namespace cta::objectstore