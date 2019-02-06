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
#include "GenericObject.hpp"
#include "AgentReference.hpp"
#include <google/protobuf/util/json_util.h>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackRequest::RepackRequest(const std::string& address, Backend& os):
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t> (os, address) { }

//------------------------------------------------------------------------------
// RepackRequest::RepackRequest()
//------------------------------------------------------------------------------
RepackRequest::RepackRequest(GenericObject& go):
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

//------------------------------------------------------------------------------
// RepackRequest::initialize()
//------------------------------------------------------------------------------
void RepackRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t>::initialize();
  m_payload.set_status(serializers::RepackRequestStatus::RRS_Pending);
  m_payload.set_expandmode(true);
  m_payload.set_repackmode(true);
  m_payload.set_totalfilestoretrieve(0);
  m_payload.set_totalbytestoretrieve(0);
  m_payload.set_totalfilestoarchive(0);
  m_payload.set_totalbytestoarchive(0);
  m_payload.set_userprovidedfiles(0);
  m_payload.set_userprovidedbytes(0);
  m_payload.set_retrievedfiles(0);
  m_payload.set_retrievedbytes(0);
  m_payload.set_archivedfiles(0);
  m_payload.set_archivedbytes(0);
  m_payload.set_failedtoretievefiles(0);
  m_payload.set_failedtoretrievebytes(0);
  m_payload.set_failedtoarchivefiles(0);
  m_payload.set_failedtoarchivebytes(0);
  m_payload.set_lastexpandedfseq(0);
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
void RepackRequest::setType(common::dataStructures::RepackInfo::Type repackType) {
  checkPayloadWritable();
  typedef common::dataStructures::RepackInfo::Type RepackType;
  switch (repackType) {
  case RepackType::ExpandAndRepack:
    // Nothing to do, this is the default case.
    break;
  case RepackType::ExpandOnly:
    m_payload.set_repackmode(false);
    break;
  case RepackType::RepackOnly:
    m_payload.set_expandmode(false);
    break;
  default:
    throw exception::Exception("In RepackRequest::setRepackType(): unexpected type.");
  }
}

//------------------------------------------------------------------------------
// RepackRequest::getInfo()
//------------------------------------------------------------------------------
common::dataStructures::RepackInfo RepackRequest::getInfo() {
  checkPayloadReadable();
  typedef common::dataStructures::RepackInfo RepackInfo;
  RepackInfo ret;
  ret.vid = m_payload.vid();
  ret.status = (RepackInfo::Status) m_payload.status();
  if (m_payload.repackmode()) {
    if (m_payload.expandmode()) {
      ret.type = RepackInfo::Type::ExpandAndRepack;
    } else {
      ret.type = RepackInfo::Type::RepackOnly;
    }
  } else if (m_payload.expandmode()) {
    ret.type = RepackInfo::Type::ExpandOnly;
  } else {
    throw exception::Exception("In RepackRequest::getInfo(): unexpcted mode: neither expand nor repack.");
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackRequest::RepackSubRequestPointer::serialize()
//------------------------------------------------------------------------------
void RepackRequest::RepackSubRequestPointer::serialize(serializers::RepackSubRequestPointer& rsrp) {
  rsrp.set_address(address);
  rsrp.set_fseq(fSeq);
  rsrp.set_retrieveaccounted(retrieveAccounted);
  rsrp.set_archiveaccounted(archiveAccounted);
  rsrp.set_failureaccounted(failureAccounted);
  rsrp.set_subrequestdeleted(subrequestDeleted);
}

//------------------------------------------------------------------------------
// RepackRequest::RepackSubRequestPointer::deserialize()
//------------------------------------------------------------------------------
void RepackRequest::RepackSubRequestPointer::deserialize(const serializers::RepackSubRequestPointer& rsrp) {
  address = rsrp.address();
  fSeq = rsrp.fseq();
  retrieveAccounted = rsrp.retrieveaccounted();
  archiveAccounted = rsrp.archiveaccounted();
  failureAccounted = rsrp.failureaccounted();
  subrequestDeleted = rsrp.subrequestdeleted();
}

//------------------------------------------------------------------------------
// RepackRequest::getOrPrepareSubrequestInfo()
//------------------------------------------------------------------------------
auto RepackRequest::getOrPrepareSubrequestInfo(std::set<uint32_t> fSeqs, AgentReference& agentRef) 
-> SubrequestInfo::set {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  SubrequestInfo::set ret;
  bool newElementCreated = false;
  // Prepare to return existing or created address.
  for (auto &fs: fSeqs) {
    SubrequestInfo retInfo;
    try {
      auto & srp = pointerMap.at(fs);
      retInfo.address = srp.address;
      retInfo.fSeq = srp.fSeq;
      retInfo.subrequestDeleted = srp.subrequestDeleted;
    } catch (std::out_of_range &) {
      retInfo.address = agentRef.nextId("repackSubRequest");
      retInfo.fSeq = fs;
      retInfo.subrequestDeleted = false;
      auto & p = pointerMap[fs];
      p.address = retInfo.address;
      p.fSeq = fs;
      p.archiveAccounted = p.retrieveAccounted = p.failureAccounted = p.subrequestDeleted = false;
      newElementCreated = true;
    }
    ret.emplace(retInfo);
  }
  // Record changes, if any.
  if (newElementCreated) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackRequest::setLastExpandedFSeq()
//------------------------------------------------------------------------------
void RepackRequest::setLastExpandedFSeq(uint64_t lastExpandedFSeq) {
  checkWritable();
  m_payload.set_lastexpandedfseq(lastExpandedFSeq);
}

//------------------------------------------------------------------------------
// RepackRequest::getLastExpandedFSeq()
//------------------------------------------------------------------------------
uint64_t RepackRequest::getLastExpandedFSeq() {
  checkPayloadReadable();
  return m_payload.lastexpandedfseq();
}

//------------------------------------------------------------------------------
// RepackRequest::reportRetriveSuccesses()
//------------------------------------------------------------------------------
void RepackRequest::reportRetriveSuccesses(SubrequestStatistics::List& retrieveSuccesses) {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  bool didUpdate = false;
  for (auto & rs: retrieveSuccesses) {
    try {
      auto & p = pointerMap.at(rs.fSeq);
      if (!p.retrieveAccounted) {
        p.retrieveAccounted = true;
        m_payload.set_retrievedbytes(m_payload.retrievedbytes() + rs.bytes);
        m_payload.set_retrievedfiles(m_payload.retrievedfiles() + rs.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportRetriveSuccesses(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportRetriveFailures()
//------------------------------------------------------------------------------
void RepackRequest::reportRetriveFailures(SubrequestStatistics::List& retrieveFailures) {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  bool didUpdate = false;
  for (auto & rs: retrieveFailures) {
    try {
      auto & p = pointerMap.at(rs.fSeq);
      if (!p.failureAccounted) {
        p.failureAccounted = true;
        m_payload.set_failedtoretrievebytes(m_payload.failedtoretrievebytes() + rs.bytes);
        m_payload.set_failedtoretievefiles(m_payload.failedtoretievefiles() + rs.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportRetriveFailures(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportArchiveSuccesses()
//------------------------------------------------------------------------------
void RepackRequest::reportArchiveSuccesses(SubrequestStatistics::List& archiveSuccesses) {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  bool didUpdate = false;
  for (auto & as: archiveSuccesses) {
    try {
      auto & p = pointerMap.at(as.fSeq);
      if (!p.archiveAccounted) {
        p.archiveAccounted = true;
        m_payload.set_archivedbytes(m_payload.archivedbytes() + as.bytes);
        m_payload.set_archivedfiles(m_payload.archivedfiles() + as.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportArchiveSuccesses(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportArchiveFailures()
//------------------------------------------------------------------------------
void RepackRequest::reportArchiveFailures(SubrequestStatistics::List& archiveFailures) {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  bool didUpdate = false;
  for (auto & rs: archiveFailures) {
    try {
      auto & p = pointerMap.at(rs.fSeq);
      if (!p.failureAccounted) {
        p.failureAccounted = true;
        m_payload.set_failedtoarchivebytes(m_payload.failedtoarchivebytes() + rs.bytes);
        m_payload.set_failedtoarchivefiles(m_payload.failedtoarchivefiles() + rs.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportRetriveFailures(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportSubRequestsForDeletion()
//------------------------------------------------------------------------------
void RepackRequest::reportSubRequestsForDeletion(std::list<uint64_t>& fSeqs) {
  checkPayloadWritable();
  RepackSubRequestPointer::Map pointerMap;
  // Read the map
  for (auto &rsrp: m_payload.subrequests()) pointerMap[rsrp.fseq()].deserialize(rsrp);
  bool didUpdate = false;
  for (auto & fs: fSeqs) {
    try {
      auto & p = pointerMap.at(fs);
      if (!p.subrequestDeleted) {
        p.subrequestDeleted = true;
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportSubRequestsForDeletion(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.deserialize(*m_payload.mutable_subrequests()->Add());
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
RepackRequest::AsyncOwnerAndStatusUpdater* RepackRequest::asyncUpdateOwnerAndStatus(const std::string& owner, const std::string& previousOwner, 
    cta::optional<serializers::RepackRequestStatus> newStatus) {
  std::unique_ptr<AsyncOwnerAndStatusUpdater> ret(new AsyncOwnerAndStatusUpdater);
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
      // Pick up info to return
      serializers::RepackRequest payload;
      if (!payload.ParseFromString(oh.payload())) {
        // Use a the tolerant parser to assess the situation.
        payload.ParsePartialFromString(oh.payload());
        throw cta::exception::Exception(std::string("In RepackRequest::asyncUpdateOwner(): could not parse payload: ")+
          payload.InitializationErrorString());
      }
      // We only need to modify the pay load if there is a status change.
      if (newStatus) payload.set_status(newStatus.value());
      typedef common::dataStructures::RepackInfo RepackInfo;
      retRef.m_repackInfo.status = (RepackInfo::Status) payload.status();
      retRef.m_repackInfo.vid = payload.vid();
      if (payload.repackmode()) {
        if (payload.expandmode()) {
          retRef.m_repackInfo.type = RepackInfo::Type::ExpandAndRepack;
        } else {
          retRef.m_repackInfo.type = RepackInfo::Type::RepackOnly;
        }
      } else if (payload.expandmode()) {
        retRef.m_repackInfo.type = RepackInfo::Type::ExpandOnly;
      } else {
        throw exception::Exception("In RepackRequest::asyncUpdateOwner()::lambda(): unexpcted mode: neither expand nor repack.");
      }
      // We only need to modify the pay load if there is a status change.
      if (newStatus) oh.set_payload(payload.SerializePartialAsString());
      return oh.SerializeAsString();
    };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(), ret->m_updaterCallback));
  return ret.release();  
}

//------------------------------------------------------------------------------
// RepackRequest::AsyncOwnerUpdater::wait()
//------------------------------------------------------------------------------
void RepackRequest::AsyncOwnerAndStatusUpdater::wait() {
  m_backendUpdater->wait();
  m_timingReport.insertAndReset("commitUnlockTime", m_timer);
}

//------------------------------------------------------------------------------
// RepackRequest::AsyncOwnerUpdater::wait()
//------------------------------------------------------------------------------
common::dataStructures::RepackInfo RepackRequest::AsyncOwnerAndStatusUpdater::getInfo() {
  return m_repackInfo;
}


//------------------------------------------------------------------------------
// RepackRequest::dump()
//------------------------------------------------------------------------------
std::string RepackRequest::dump() {  
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

}} // namespace cta::objectstore