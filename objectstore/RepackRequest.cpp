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

#include <google/protobuf/util/json_util.h>
#include <iostream>

#include "AgentReference.hpp"
#include "AgentWrapper.hpp"
#include "Algorithms.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "GenericObject.hpp"
#include "MountPolicySerDeser.hpp"
#include "RepackQueueAlgorithms.hpp"
#include "RepackRequest.hpp"
#include "disk/DiskFile.hpp"

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
  m_payload.set_failedtocreatearchivereq(0);
  m_payload.set_failedtoarchivefiles(0);
  m_payload.set_failedtoarchivebytes(0);
  m_payload.set_lastexpandedfseq(0);
  m_payload.set_is_expand_finished(false);
  m_payload.set_is_expand_started(false);
  m_payload.set_force_disabled_tape(false); // TODO: To remove after REPACKING state is fully deployed
  m_payload.set_no_recall(false);
  m_payload.set_is_complete(false);
  m_payload.set_repack_finished_time(0);
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
  if(repackStatus == common::dataStructures::RepackInfo::Status::Complete || repackStatus == common::dataStructures::RepackInfo::Status::Failed){
      m_payload.set_repack_finished_time(time(nullptr));
  }
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
  ret.totalFilesToArchive = m_payload.totalfilestoarchive();
  ret.totalBytesToArchive = m_payload.totalbytestoarchive();
  ret.totalFilesToRetrieve = m_payload.totalfilestoretrieve();
  ret.totalBytesToRetrieve = m_payload.totalbytestoretrieve();
  ret.failedFilesToArchive = m_payload.failedtoarchivefiles();
  ret.failedBytesToArchive = m_payload.failedtoarchivebytes();
  ret.failedFilesToRetrieve = m_payload.failedtoretrievefiles();
  ret.failedBytesToRetrieve = m_payload.failedtoretrievebytes();
  ret.archivedFiles = m_payload.archivedfiles();
  ret.archivedBytes = m_payload.archivedbytes();
  ret.retrievedFiles = m_payload.retrievedfiles();
  ret.retrievedBytes = m_payload.retrievedbytes();
  ret.lastExpandedFseq = m_payload.lastexpandedfseq();
  ret.userProvidedFiles = m_payload.userprovidedfiles();
  ret.isExpandFinished = m_payload.is_expand_finished();
  ret.noRecall = m_payload.no_recall();
  EntryLogSerDeser creationLog;
  creationLog.deserialize(m_payload.creation_log());
  ret.creationLog = creationLog;
  ret.repackFinishedTime = m_payload.repack_finished_time();
  for(auto & rdi: m_payload.destination_infos()){
    RepackInfo::RepackDestinationInfo rdiToInsert;
    rdiToInsert.vid = rdi.vid();
    rdiToInsert.files = rdi.files();
    rdiToInsert.bytes = rdi.bytes();
    ret.destinationInfos.emplace_back(rdiToInsert);
  }
  if (m_payload.move_mode()) {
    if (m_payload.add_copies_mode()) {
      ret.type = RepackInfo::Type::MoveAndAddCopies;
    } else {
      ret.type = RepackInfo::Type::MoveOnly;
    }
  } else if (m_payload.add_copies_mode()) {
    ret.type = RepackInfo::Type::AddCopiesOnly;
  } else {
    throw exception::Exception("In RepackRequest::getInfo(): unexpected mode: neither expand nor repack.");
  }
  return ret;
}

//------------------------------------------------------------------------------
// RepackRequest::setExpandFinished()
//------------------------------------------------------------------------------
void RepackRequest::setExpandFinished(const bool expandFinished){
  checkPayloadWritable();
  m_payload.set_is_expand_finished(expandFinished);
}

void RepackRequest::setExpandStarted(const bool expandStarted){
  checkPayloadWritable();
  m_payload.set_is_expand_started(expandStarted);
}

void RepackRequest::setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles){
  setTotalFileToRetrieve(totalStatsFiles.totalFilesToRetrieve);
  setTotalFileToArchive(totalStatsFiles.totalFilesToArchive);
  setTotalBytesToArchive(totalStatsFiles.totalBytesToArchive);
  setTotalBytesToRetrieve(totalStatsFiles.totalBytesToRetrieve);
  setUserProvidedFiles(totalStatsFiles.userProvidedFiles);
}

void RepackRequest::setMountPolicy(const common::dataStructures::MountPolicy& mp){
  checkPayloadWritable();
  MountPolicySerDeser mpSerDeser(mp);
  mpSerDeser.serialize(*m_payload.mutable_mount_policy());
  m_payload.set_mountpolicyname(mp.name);
}

common::dataStructures::MountPolicy RepackRequest::getMountPolicy(){
  checkPayloadReadable();
  MountPolicySerDeser mpSerDeser;
  mpSerDeser.deserialize(m_payload.mount_policy());
  return mpSerDeser;
}

void RepackRequest::deleteAllSubrequests(log::LogContext & lc) {
  struct SubrequestDeleter{
    std::string subrequestAddr;
    std::future<void> deleterFuture;
  };
  checkPayloadWritable();
  if(!m_payload.is_complete()){
    m_payload.mutable_destination_infos()->Clear();
    auto subrequests = m_payload.mutable_subrequests();
    //we will do the deletion by batch of 500
    auto itor = subrequests->begin();
    while(itor != subrequests->end()){
      try {
        std::list<SubrequestDeleter> deleterList;
        int nbSubReqProcessessed = 0;
        while(itor != subrequests->end() && nbSubReqProcessessed < 500){
          auto & subrequest = *itor;
          deleterList.push_back({
                  subrequest.address(),
                  std::async(std::launch::async,
                             [this, subrequest](){
                    bool isArchiveRequest = subrequest.retrieve_accounted();
                    std::string fileBufferUrl;
                    if (isArchiveRequest) {
                      try {
                        cta::objectstore::ArchiveRequest ar(subrequest.address(), m_objectStore);
                        ar.fetchNoLock();
                        fileBufferUrl = ar.getRepackInfo().fileBufferURL;
                      } catch (objectstore::ObjectOpsBase::WrongType &) {
                        // If the object is of the wrong type, it may be actually a retrieve request
                        isArchiveRequest = false;
                      } catch (cta::exception::NoSuchObject &) {
                      }
                    }
                    if (!isArchiveRequest) {
                      try {
                        cta::objectstore::RetrieveRequest rr(subrequest.address(), m_objectStore);
                        rr.fetchNoLock();
                        fileBufferUrl = rr.getRepackInfo().fileBufferURL;
                      } catch (objectstore::ObjectOpsBase::WrongType &) {
                      } catch (cta::exception::NoSuchObject &) {
                      }
                    }
                    m_objectStore.remove(subrequest.address());
                    if(!fileBufferUrl.empty()) {
                      cta::disk::DiskFileRemoverFactory diskFileRemoverFactory;
                      auto fileRemover = diskFileRemoverFactory.createDiskFileRemover(fileBufferUrl);
                      fileRemover->remove();
                    }
                  })});
          subrequest.set_subrequest_deleted(true);
          nbSubReqProcessessed++;
          itor++;
        }
        for(auto & deleter: deleterList){
          try {
            deleter.deleterFuture.get();
          } catch(cta::exception::Exception &ex) {
            log::ScopedParamContainer spc(lc);
            spc.add("subrequestAddress", deleter.subrequestAddr)
               .add("exceptionMsg", ex.getMessageValue());
            lc.log(cta::log::ERR, "Failed to fully delete repack subrequest");
          }
        }
      } catch(cta::exception::NoSuchObject & ){ /* If object already deleted, do nothing */ }
    }
  }
}

void RepackRequest::setIsComplete(const bool isComplete){
  checkPayloadWritable();
  m_payload.set_is_complete(isComplete);
}

void RepackRequest::updateRepackDestinationInfos(const cta::common::dataStructures::ArchiveFile & archiveFile, const std::string & destinationVid){
  checkPayloadWritable();

  bool rdiFound = false;
  cta::objectstore::serializers::RepackDestinationInfo * info = nullptr;
  for (auto rdiIter = m_payload.mutable_destination_infos()->begin(); !rdiFound && rdiIter != m_payload.mutable_destination_infos()->end(); ++rdiIter) {
    //find the infos for the vid of the archived file
    if(rdiIter->vid() == destinationVid){
      info = &(*rdiIter);
      rdiFound = true;
    }
  }
  if(!rdiFound){
    //info has to be created, create it and set its vid
    //by default the files and the bytes = 0 (see cta.proto)
    info = m_payload.mutable_destination_infos()->Add();
    info->set_vid(destinationVid);
  }
  info->set_files(info->files() + 1);
  info->set_bytes(info->bytes() + archiveFile.fileSize);
}

std::list<common::dataStructures::RepackInfo::RepackDestinationInfo> RepackRequest::getRepackDestinationInfos(){
  checkPayloadReadable();

  std::list<common::dataStructures::RepackInfo::RepackDestinationInfo> ret;
  for(auto rdiIter: m_payload.destination_infos()){
    common::dataStructures::RepackInfo::RepackDestinationInfo rdi;
    rdi.vid = rdiIter.vid();
    rdi.files = rdiIter.files();
    rdi.bytes = rdiIter.bytes();
    ret.emplace_back(rdi);
  }
  return ret;
}

void RepackRequest::setCreationLog(const common::dataStructures::EntryLog & creationLog){
  checkPayloadWritable();
  cta::objectstore::EntryLogSerDeser creationLogToSet(creationLog);
  creationLogToSet.serialize(*m_payload.mutable_creation_log());
}

common::dataStructures::EntryLog RepackRequest::getCreationLog() {
  checkPayloadReadable();
  cta::objectstore::EntryLogSerDeser ret;
  ret.deserialize(m_payload.creation_log());
  return ret;
}

void RepackRequest::setNoRecall(const bool noRecall) {
  checkPayloadWritable();
  m_payload.set_no_recall(noRecall);
}

bool RepackRequest::getNoRecall(){
  checkPayloadReadable();
  return m_payload.no_recall();
}

void RepackRequest::setStatus(){
  checkPayloadWritable();
  checkPayloadReadable();
  if(m_payload.is_expand_started()){
    //The expansion of the Repack Request have started
    if(m_payload.is_expand_finished()){
      if( (m_payload.retrievedfiles() + m_payload.failedtoretrievefiles() >= m_payload.totalfilestoretrieve()) && (m_payload.archivedfiles() + m_payload.failedtoarchivefiles() + m_payload.failedtocreatearchivereq() >= m_payload.totalfilestoarchive()) ){
        //We reached the end
        if (m_payload.failedtoretrievefiles() || m_payload.failedtoarchivefiles()) {
          //At least one retrieve or archive has failed
          m_payload.set_repack_finished_time(time(nullptr));
          setStatus(common::dataStructures::RepackInfo::Status::Failed);
        } else {
          //No Failure, we are status Complete
          m_payload.set_repack_finished_time(time(nullptr));
          setStatus(common::dataStructures::RepackInfo::Status::Complete);
        }
        removeFromOwnerAgentOwnership();
        return;
      }
    }
    //Expand is finished or not, if we have retrieved files or not (first reporting) or if we have archived files (repack tape repair workflow with all files
    //provided by the user), we are in Running,
    //else we are in starting
    if(m_payload.retrievedfiles() || m_payload.failedtoretrievefiles() || m_payload.archivedfiles() || m_payload.failedtoarchivefiles()){
      setStatus(common::dataStructures::RepackInfo::Status::Running);
    } else {
      setStatus(common::dataStructures::RepackInfo::Status::Starting);
    }
  }
}

//------------------------------------------------------------------------------
// RepackRequest::getExpandFinished()
//------------------------------------------------------------------------------
bool RepackRequest::isExpandFinished(){
  checkPayloadReadable();
  return m_payload.is_expand_finished();
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

void RepackRequest::removeFromOwnerAgentOwnership(){
  checkPayloadReadable();
  checkPayloadWritable();
  cta::objectstore::Agent ag(getOwner(),m_objectStore);
  cta::objectstore::AgentWrapper agWrapper(ag);
  agWrapper.removeFromOwnership(getAddressIfSet(),m_objectStore);
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
// RepackRequest::setTotalFileToRetrieve()
//------------------------------------------------------------------------------
void RepackRequest::setTotalFileToRetrieve(const uint64_t nbFilesToRetrieve){
  checkPayloadWritable();
  m_payload.set_totalfilestoretrieve(nbFilesToRetrieve);
}

//------------------------------------------------------------------------------
// RepackRequest::setTotalBytesToRetrieve()
//------------------------------------------------------------------------------
void RepackRequest::setTotalBytesToRetrieve(const uint64_t nbBytesToRetrieve){
  checkPayloadWritable();
  m_payload.set_totalbytestoretrieve(nbBytesToRetrieve);
}

//------------------------------------------------------------------------------
// RepackRequest::setTotalFileToArchive()
//------------------------------------------------------------------------------
void RepackRequest::setTotalFileToArchive(const uint64_t nbFilesToArchive){
  checkPayloadWritable();
  m_payload.set_totalfilestoarchive(nbFilesToArchive);
}

//------------------------------------------------------------------------------
// RepackRequest::setTotalBytesToArchive()
//------------------------------------------------------------------------------
void RepackRequest::setTotalBytesToArchive(const uint64_t nbBytesToArchive) {
  checkPayloadWritable();
  m_payload.set_totalbytestoarchive(nbBytesToArchive);
}

void RepackRequest::setUserProvidedFiles(const uint64_t userProvidedFiles){
  checkPayloadWritable();
  m_payload.set_userprovidedfiles(userProvidedFiles);
}

//------------------------------------------------------------------------------
// RepackRequest::getTotalStatsFile()
//------------------------------------------------------------------------------
cta::SchedulerDatabase::RepackRequest::TotalStatsFiles RepackRequest::getTotalStatsFile() {
  checkPayloadReadable();
  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles ret;
  ret.totalBytesToRetrieve = m_payload.totalbytestoretrieve();
  ret.totalBytesToArchive = m_payload.totalbytestoarchive();
  ret.totalFilesToRetrieve = m_payload.totalfilestoretrieve();
  ret.totalFilesToArchive = m_payload.totalfilestoarchive();
  ret.userProvidedFiles = m_payload.userprovidedfiles();
  return ret;
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
        if(!rs.hasUserProvidedFile){
          m_payload.set_retrievedbytes(m_payload.retrievedbytes() + rs.bytes);
          m_payload.set_retrievedfiles(m_payload.retrievedfiles() + rs.files);
        }
        didUpdate = true;
      }
    } catch (std::out_of_range &) {
      throw exception::Exception("In RepackRequest::reportRetriveSuccesses(): got a report for unknown fSeq");
    }
  }
  if (didUpdate) {
    setStatus();
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
    setStatus();
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
  }
}

//------------------------------------------------------------------------------
// RepackRequest::reportArchiveSuccesses()
// Returns the status of the RepackRequest
//------------------------------------------------------------------------------
serializers::RepackRequestStatus RepackRequest::reportArchiveSuccesses(SubrequestStatistics::List& archiveSuccesses) {
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
        cta::objectstore::ArchiveRequest ar(p.address,m_objectStore);
        ar.fetchNoLock();
        updateRepackDestinationInfos(ar.getArchiveFile(),as.destinationVid);
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
    setStatus();
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
  }
  return m_payload.status();
}

//------------------------------------------------------------------------------
// RepackRequest::reportArchiveFailures()
//------------------------------------------------------------------------------
serializers::RepackRequestStatus RepackRequest::reportArchiveFailures(SubrequestStatistics::List& archiveFailures) {
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
    setStatus();
    m_payload.mutable_subrequests()->Clear();
    for (auto & p: pointerMap) p.second.serialize(*m_payload.mutable_subrequests()->Add());
  }
  return m_payload.status();
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
// RepackRequest::reportRetrieveCreationFailures()
//------------------------------------------------------------------------------
void RepackRequest::reportRetrieveCreationFailures(const std::list<cta::SchedulerDatabase::RepackRequest::Subrequest>& notCreatedSubrequests){
  checkPayloadWritable();
  checkPayloadReadable();
  uint64_t failedToRetrieveFiles = 0, failedToRetrieveBytes = 0, failedToCreateArchiveReq = 0;
  for(auto & subreq: notCreatedSubrequests){
    failedToRetrieveFiles++;
    failedToRetrieveBytes+=subreq.archiveFile.fileSize;
    for(auto & copyNb: subreq.copyNbsToRearchive){
      (void) copyNb;
      failedToCreateArchiveReq++;
    }
  }
  m_payload.set_failedtoretrievebytes(m_payload.failedtoretrievebytes() + failedToRetrieveBytes);
  m_payload.set_failedtoretrievefiles(m_payload.failedtoretrievefiles() + failedToRetrieveFiles);
  reportArchiveCreationFailures(failedToCreateArchiveReq);
  setStatus();
}

void RepackRequest::reportArchiveCreationFailures(uint64_t nbFailedToCreateArchiveRequests){
  checkPayloadWritable();
  m_payload.set_failedtocreatearchivereq(m_payload.failedtocreatearchivereq() + nbFailedToCreateArchiveRequests);
}

//------------------------------------------------------------------------------
// RepackRequest::garbageCollect()
//------------------------------------------------------------------------------
void RepackRequest::garbageCollect(const std::string& presumedOwner, AgentReference& agentReference,
    log::LogContext& lc, cta::catalogue::Catalogue& catalogue) {
  //Let's requeue the RepackRequest if its status is ToExpand or Pending
  agentReference.addToOwnership(this->getAddressIfSet(), m_objectStore);
  //The owner of this RepackRequest is the agentReference
  setOwner(agentReference.getAgentAddress());
  cta::utils::Timer t;
  RepackQueue rq(m_objectStore);
  ScopedExclusiveLock rql;
  try{
    Helpers::getLockedAndFetchedRepackQueue(rq, rql, agentReference, this->getInfo().getQueueType(), lc);
  } catch(const cta::exception::Exception &e){
    lc.log(log::INFO,"In RepackRequest::garbageCollect(): failed to requeue the RepackRequest (leaving it as it is) : "+e.getMessage().str());
    commit();
    return;
  }
  double queueLockFetchTime = t.secs(utils::Timer::resetCounter);
  auto jobsSummary = rq.getRequestsSummary();
  uint64_t requestsBefore = jobsSummary.requests;
  std::list<std::string> requestsToAdd;
  requestsToAdd.push_back(this->getAddressIfSet());
  try{
    rq.addRequestsAndCommit(requestsToAdd,lc);
    jobsSummary = rq.getRequestsSummary();
    uint64_t requestsAfter = jobsSummary.requests;
    log::ScopedParamContainer params(lc);
    params.add("queueLockFetchTime",queueLockFetchTime)
          .add("queueAddress",rq.getAddressIfSet())
          .add("requestsBefore",requestsBefore)
          .add("requestsAfter",requestsAfter);
    lc.log(log::INFO,"In RepackRequest::garbageCollect() succesfully requeued the RepackRequest.");
  } catch(const cta::exception::Exception &e){
    lc.log(log::INFO,"In RepackRequest::garbageCollect() failed to requeue the RepackRequest. Leaving it as it is.");
  }
  commit();
}

//------------------------------------------------------------------------------
// RepackRequest::asyncUpdateOwner()
//------------------------------------------------------------------------------
RepackRequest::AsyncOwnerAndStatusUpdater* RepackRequest::asyncUpdateOwnerAndStatus(const std::string& owner, const std::string& previousOwner,
    std::optional<serializers::RepackRequestStatus> newStatus) {
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
      retRef.m_repackInfo.noRecall = payload.no_recall();
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
      if (newStatus) oh.set_payload(payload.SerializeAsString());
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
