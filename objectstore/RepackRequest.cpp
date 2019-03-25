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
// Constructor
//------------------------------------------------------------------------------
RepackRequest::RepackRequest(Backend& os):
  ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t> (os) { }


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
  m_payload.set_add_copies_mode(true);
  m_payload.set_move_mode(true);
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
  m_payload.set_failedtoretrievefiles(0);
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
  case RepackType::MoveAndAddCopies:
    // Nothing to do, this is the default case.
    break;
  case RepackType::AddCopiesOnly:
    m_payload.set_move_mode(false);
    break;
  case RepackType::MoveOnly:
    m_payload.set_add_copies_mode(false);
    break;
  default:
    throw exception::Exception("In RepackRequest::setRepackType(): unexpected type.");
  }
}

//------------------------------------------------------------------------------
// RepackRequest::setStatus()
//------------------------------------------------------------------------------
void RepackRequest::setStatus(common::dataStructures::RepackInfo::Status repackStatus) {
  checkPayloadWritable();
  // common::dataStructures::RepackInfo::Status and serializers::RepackRequestStatus are defined using the same values,
  // hence the cast.
  m_payload.set_status((serializers::RepackRequestStatus)repackStatus);
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
  ret.repackBufferBaseURL = m_payload.buffer_url();
  if (m_payload.move_mode()) {
    if (m_payload.add_copies_mode()) {
      ret.type = RepackInfo::Type::MoveAndAddCopies;
    } else {
      ret.type = RepackInfo::Type::MoveOnly;
    }
  } else if (m_payload.add_copies_mode()) {
    ret.type = RepackInfo::Type::AddCopiesOnly;
  } else {
    throw exception::Exception("In RepackRequest::getInfo(): unexpcted mode: neither expand nor repack.");
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackRequest::setBufferURL()
//------------------------------------------------------------------------------
void RepackRequest::setBufferURL(const std::string& bufferURL) {
  checkPayloadWritable();
  m_payload.set_buffer_url(bufferURL);
}

//------------------------------------------------------------------------------
// RepackRequest::RepackSubRequestPointer::serialize()
//------------------------------------------------------------------------------
void RepackRequest::RepackSubRequestPointer::serialize(serializers::RepackSubRequestPointer& rsrp) {
  rsrp.set_address(address);
  rsrp.set_fseq(fSeq);
  rsrp.set_retrieve_accounted(retrieveAccounted);
  rsrp.mutable_archive_copynb_accounted()->Clear();
  for (auto cna: archiveCopyNbsAccounted) { rsrp.mutable_archive_copynb_accounted()->Add(cna); }
  rsrp.set_subrequest_deleted(subrequestDeleted);
}

//------------------------------------------------------------------------------
// RepackRequest::RepackSubRequestPointer::deserialize()
//------------------------------------------------------------------------------
void RepackRequest::RepackSubRequestPointer::deserialize(const serializers::RepackSubRequestPointer& rsrp) {
  address = rsrp.address();
  fSeq = rsrp.fseq();
  retrieveAccounted = rsrp.retrieve_accounted();
  archiveCopyNbsAccounted.clear();
  for (auto acna: rsrp.archive_copynb_accounted()) { archiveCopyNbsAccounted.insert(acna); }
  subrequestDeleted = rsrp.subrequest_deleted();
}

//------------------------------------------------------------------------------
// RepackRequest::getOrPrepareSubrequestInfo()
//------------------------------------------------------------------------------
auto RepackRequest::getOrPrepareSubrequestInfo(std::set<uint64_t> fSeqs, AgentReference& agentRef) 
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
      retInfo.address = agentRef.nextId("RepackSubRequest");
      retInfo.fSeq = fs;
      retInfo.subrequestDeleted = false;
      auto & p = pointerMap[fs];
      p.address = retInfo.address;
      p.fSeq = fs;
      p.retrieveAccounted = p.subrequestDeleted = false;
      p.archiveCopyNbsAccounted.clear();
      newElementCreated = true;
    }
    ret.emplace(retInfo);
  }
  // Record changes, if any.
  if (newElementCreated) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackRequest::setLastExpandedFSeq()
//------------------------------------------------------------------------------
void RepackRequest::setLastExpandedFSeq(uint64_t lastExpandedFSeq) {
  checkPayloadWritable();
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
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
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
  for (auto & rf: retrieveFailures) {
    try {
      auto & p = pointerMap.at(rf.fSeq);
      if (!p.retrieveAccounted) {
        p.retrieveAccounted = true;
        m_payload.set_failedtoretrievebytes(m_payload.failedtoretrievebytes() + rf.bytes);
        m_payload.set_failedtoretrievefiles(m_payload.failedtoretrievefiles() + rf.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportRetriveFailures(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
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
      if (!p.archiveCopyNbsAccounted.count(as.copyNb)) {
        p.archiveCopyNbsAccounted.insert(as.copyNb);
        m_payload.set_archivedbytes(m_payload.archivedbytes() + as.bytes);
        m_payload.set_archivedfiles(m_payload.archivedfiles() + as.files);
        p.subrequestDeleted = as.subrequestDeleted;
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportArchiveSuccesses(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    // Check whether we reached the end.
    if (m_payload.archivedfiles() + m_payload.failedtoarchivefiles() >= m_payload.totalfilestoarchive()) {
      if (m_payload.failedtoarchivefiles()) {
        m_payload.set_status(serializers::RepackRequestStatus::RRS_Failed);
      } else {
        m_payload.set_status(serializers::RepackRequestStatus::RRS_Complete);
      }
    }
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
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
  for (auto & af: archiveFailures) {
    try {
      auto & p = pointerMap.at(af.fSeq);
      if (!p.archiveCopyNbsAccounted.count(af.copyNb)) {
        p.archiveCopyNbsAccounted.insert(af.copyNb);
        p.subrequestDeleted = true;
        m_payload.set_failedtoarchivebytes(m_payload.failedtoarchivebytes() + af.bytes);
        m_payload.set_failedtoarchivefiles(m_payload.failedtoarchivefiles() + af.files);
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportArchiveFailures(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    // Check whether we reached the end.
    if (m_payload.archivedfiles() + m_payload.failedtoarchivefiles() >= m_payload.totalfilestoarchive())
      m_payload.set_status(serializers::RepackRequestStatus::RRS_Failed);
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
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
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportSubRequestsForDeletion()
//------------------------------------------------------------------------------
auto RepackRequest::getStats() -> std::map<StatsType, StatsValues> {
  checkPayloadReadable();
  std::map<StatsType, StatsValues> ret;
  ret[StatsType::ArchiveTotal].files = m_payload.totalfilestoarchive();
  ret[StatsType::ArchiveTotal].bytes = m_payload.totalbytestoarchive();
  ret[StatsType::RetrieveTotal].files = m_payload.totalfilestoretrieve();
  ret[StatsType::RetrieveTotal].bytes = m_payload.totalbytestoretrieve();
  ret[StatsType::UserProvided].files = m_payload.userprovidedfiles();
  ret[StatsType::UserProvided].bytes = m_payload.userprovidedbytes();
  ret[StatsType::RetrieveFailure].files = m_payload.failedtoretrievefiles();
  ret[StatsType::RetrieveFailure].bytes = m_payload.failedtoretrievebytes();
  ret[StatsType::RetrieveSuccess].files = m_payload.retrievedfiles();
  ret[StatsType::RetrieveSuccess].bytes = m_payload.retrievedbytes();
  ret[StatsType::ArchiveFailure].files = m_payload.failedtoarchivefiles();
  ret[StatsType::ArchiveFailure].bytes = m_payload.failedtoarchivebytes();
  ret[StatsType::ArchiveSuccess].files = m_payload.archivedfiles();
  ret[StatsType::ArchiveSuccess].bytes = m_payload.archivedbytes();
  return ret;
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
      retRef.m_repackInfo.repackBufferBaseURL = payload.buffer_url();
      if (payload.move_mode()) {
        if (payload.add_copies_mode()) {
          retRef.m_repackInfo.type = RepackInfo::Type::MoveAndAddCopies;
        } else {
          retRef.m_repackInfo.type = RepackInfo::Type::MoveOnly;
        }
      } else if (payload.add_copies_mode()) {
        retRef.m_repackInfo.type = RepackInfo::Type::AddCopiesOnly;
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