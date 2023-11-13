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

#include <iostream>

#include "common/exception/NoSuchObject.hpp"
#include "common/threading/MutexLocker.hpp"
#include "Helpers.hpp"
#include "Sorter.hpp"

namespace cta::objectstore {

/* SORTER CLASS */

Sorter::Sorter(AgentReference& agentReference, Backend& objectstore, catalogue::Catalogue& catalogue)
  : m_agentReference(agentReference), m_objectstore(objectstore), m_catalogue(catalogue) {
}

Sorter::~Sorter() {
}
/* Archive related algorithms */

template <typename SpecificQueue>
void Sorter::executeArchiveAlgorithm(const std::string& tapePool, std::string& queueAddress,
  std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext& lc) {
  typedef ContainerAlgorithms<ArchiveQueue, SpecificQueue> Algo;
  Algo algo(m_objectstore, m_agentReference);
  typename Algo::InsertedElement::list jobsToAdd;
  std::map<uint64_t, std::shared_ptr<ArchiveJobQueueInfo>> succeededJobs;
  std::string previousOwner;
  for (auto& jobToAdd : jobs) {
    SorterArchiveJob job = std::get<0>(jobToAdd->jobToQueue);
    succeededJobs[job.jobDump.copyNb] = jobToAdd;
    previousOwner = job.previousOwner->getAgentAddress();
    jobsToAdd.push_back({ job.archiveRequest.get(), job.jobDump.copyNb, job.archiveFile, job.mountPolicy, std::nullopt });
  }
  try {
    algo.referenceAndSwitchOwnership(tapePool, previousOwner, jobsToAdd, lc);
  } catch (typename Algo::OwnershipSwitchFailure &failure) {
    for (auto &failedAR : failure.failedElements) {
      try {
        std::rethrow_exception(failedAR.failure);
      } catch (const cta::exception::NoSuchObject &ex) {
        log::ScopedParamContainer params(lc);
        params.add("fileId", failedAR.element->archiveFile.archiveFileID);
        lc.log(log::WARNING, "In Sorter::executeArchiveAlgorithm(), queueing impossible, job do not exist in the objectstore.");
      } catch (const cta::exception::Exception &e) {
        uint32_t copyNb = failedAR.element->copyNb;
        std::get<1>(succeededJobs[copyNb]->jobToQueue).set_exception(std::current_exception());
        succeededJobs.erase(copyNb);
      }
    }
  }
  for (auto& succeededJob : succeededJobs) {
    std::get<1>(succeededJob.second->jobToQueue).set_value();
  }
}

void Sorter::dispatchArchiveAlgorithm(const std::string& tapePool, const common::dataStructures::JobQueueType& jobQueueType,
  std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext &lc) {
  switch (jobQueueType) {
    case common::dataStructures::JobQueueType::JobsToReportToUser:
      executeArchiveAlgorithm<ArchiveQueueToReportForUser>(tapePool, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToTransferForRepack:
      executeArchiveAlgorithm<ArchiveQueueToTransferForRepack>(tapePool,  queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
      executeArchiveAlgorithm<ArchiveQueueToReportToRepackForSuccess>(tapePool, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
      executeArchiveAlgorithm<ArchiveQueueToReportToRepackForFailure>(tapePool, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToTransferForUser:
      executeArchiveAlgorithm<ArchiveQueueToTransferForUser>(tapePool, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::FailedJobs:
      executeArchiveAlgorithm<ArchiveQueueFailed>(tapePool, queueAddress, jobs, lc);
      break;
    default:
      throw cta::exception::Exception("In Sorter::dispatchArchiveAlgorithm(), unknown JobQueueType");
  }
}

void Sorter::insertArchiveRequest(std::shared_ptr<ArchiveRequest> archiveRequest,
  AgentReferenceInterface& previousOwner, log::LogContext& lc) {
  for (auto& job : archiveRequest->dumpJobs()) {
    SorterArchiveJob jobToInsert;
    jobToInsert.archiveRequest = archiveRequest;
    jobToInsert.archiveFile = archiveRequest->getArchiveFile();
    jobToInsert.jobDump = job;
    jobToInsert.mountPolicy = archiveRequest->getMountPolicy();
    jobToInsert.previousOwner = &previousOwner;
    try {
      jobToInsert.jobQueueType = archiveRequest->getJobQueueType(job.copyNb);
      insertArchiveJob(jobToInsert);
    } catch (const cta::exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
      params.add("fileId", archiveRequest->getArchiveFile().archiveFileID)
            .add("exceptionMessage", ex.getMessageValue())
            .add("status", ArchiveRequest::statusToString(job.status));
      lc.log(log::ERR, "In Sorter::insertArchiveJob() Failed to determine destination queue for Archive Job.");
    }
  }
}

void Sorter::insertArchiveRequest(const SorterArchiveRequest& archiveRequest, AgentReferenceInterface& previousOwner, log::LogContext& lc) {
  for(auto& archiveJob: archiveRequest.archiveJobs){
    SorterArchiveJob jobToInsert = archiveJob;
    jobToInsert.previousOwner = &previousOwner;
    insertArchiveJob(jobToInsert);
  }
}

void Sorter::insertArchiveJob(const SorterArchiveJob& job){
  auto ajqi = std::make_shared<ArchiveJobQueueInfo>();
  ajqi->jobToQueue = std::make_tuple(job,std::promise<void>());
  threading::MutexLocker mapLocker(m_mutex);
  m_archiveQueuesAndRequests[std::make_tuple(job.jobDump.tapePool, job.jobQueueType)].emplace_back(ajqi);
}

bool Sorter::flushOneArchive(log::LogContext &lc) {
  threading::MutexLocker locker(m_mutex);
  for(auto & kv: m_archiveQueuesAndRequests){
    if(!kv.second.empty()){
      queueArchiveRequests(std::get<0>(kv.first),std::get<1>(kv.first),kv.second,lc);
      m_archiveQueuesAndRequests.erase(kv.first);
      return true;
    }
  }
  return false;
}

Sorter::MapArchive Sorter::getAllArchive(){
  threading::MutexLocker mapLocker(m_mutex);
  return m_archiveQueuesAndRequests;
}

void Sorter::queueArchiveRequests(const std::string& tapePool, const common::dataStructures::JobQueueType& jobQueueType,
  std::list<std::shared_ptr<ArchiveJobQueueInfo>>& archiveJobsInfos, log::LogContext &lc) {
  std::string queueAddress;
  this->dispatchArchiveAlgorithm(tapePool, jobQueueType, queueAddress, archiveJobsInfos, lc);
}

/* End of archive related algorithms */

/* Retrieve related algorithms */

template <typename SpecificQueue>
void Sorter::executeRetrieveAlgorithm(const std::string& vid, std::string& queueAddress,
  std::list<std::shared_ptr<RetrieveJobQueueInfo>>& jobs, log::LogContext& lc) {
  typedef ContainerAlgorithms<RetrieveQueue,SpecificQueue> Algo;
  Algo algo(m_objectstore,m_agentReference);
  typename Algo::InsertedElement::list jobsToAdd;
  std::map<uint64_t, std::shared_ptr<RetrieveJobQueueInfo>> succeededJobs;
  std::string previousOwner;
  for(auto& jobToAdd: jobs){
    Sorter::RetrieveJob job = std::get<0>(jobToAdd->jobToQueue);
    succeededJobs[job.jobDump.copyNb] = jobToAdd;
    previousOwner = job.previousOwner->getAgentAddress();
    jobsToAdd.push_back({job.retrieveRequest.get(),job.jobDump.copyNb,job.fSeq,job.fileSize,job.mountPolicy,job.activity,job.diskSystemName});
  }
  try{
    algo.referenceAndSwitchOwnership(vid,previousOwner,jobsToAdd,lc);
  } catch(typename Algo::OwnershipSwitchFailure &failure){
    for(auto& failedRR: failure.failedElements){
      try {
        std::rethrow_exception(failedRR.failure);
      } catch (const cta::exception::NoSuchObject &ex) {
        log::ScopedParamContainer params(lc);
        params.add("copyNb",failedRR.element->copyNb)
              .add("fSeq",failedRR.element->fSeq);
        lc.log(log::WARNING,"In Sorter::executeRetrieveAlgorithm(), queueing impossible, job do not exist in the objectstore.");
      } catch (const cta::exception::Exception&){
        uint32_t copyNb = failedRR.element->copyNb;
        std::get<1>(succeededJobs[copyNb]->jobToQueue).set_exception(std::current_exception());
        succeededJobs.erase(copyNb);
      }
    }
  }
  for(auto &succeededJob: succeededJobs){
    std::get<1>(succeededJob.second->jobToQueue).set_value();
  }
}

void Sorter::dispatchRetrieveAlgorithm(const std::string& vid, const common::dataStructures::JobQueueType& jobQueueType,
  std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo> >& jobs, log::LogContext& lc) {
  switch (jobQueueType) {
    case common::dataStructures::JobQueueType::JobsToReportToUser:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportForUser>(vid, queueAddress, jobs, lc);
    break;
    case common::dataStructures::JobQueueType::JobsToTransferForUser:
      this->executeRetrieveAlgorithm<RetrieveQueueToTransfer>(vid, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportToRepackForSuccess>(vid, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::JobsToReportToRepackForFailure:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportToRepackForFailure>(vid, queueAddress, jobs, lc);
      break;
    case common::dataStructures::JobQueueType::FailedJobs:
      this->executeRetrieveAlgorithm<RetrieveQueueFailed>(vid, queueAddress, jobs, lc);
      break;
    default:
      throw cta::exception::Exception("In Sorter::dispatchRetrieveAlgorithm(), unknown JobQueueType");
  }
}

Sorter::RetrieveJob Sorter::createRetrieveJob(std::shared_ptr<RetrieveRequest> retrieveRequest,
  const cta::common::dataStructures::ArchiveFile& archiveFile,
  const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner) {
  Sorter::RetrieveJob jobToAdd;
  jobToAdd.jobDump.copyNb = copyNb;
  jobToAdd.fSeq = fSeq;
  jobToAdd.mountPolicy = retrieveRequest->getRetrieveFileQueueCriteria().mountPolicy;
  jobToAdd.retrieveRequest = retrieveRequest;
  jobToAdd.previousOwner = previousOwner;
  jobToAdd.jobDump.status = retrieveRequest->getJobStatus(jobToAdd.jobDump.copyNb);
  jobToAdd.fileSize = archiveFile.fileSize;
  jobToAdd.jobQueueType = retrieveRequest->getQueueType(copyNb);  // May throw an exception
  jobToAdd.activity = retrieveRequest->getActivity();
  jobToAdd.diskSystemName = retrieveRequest->getDiskSystemName();
  return jobToAdd;
}

void Sorter::insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, AgentReferenceInterface& previousOwner, std::optional<uint32_t> copyNb, log::LogContext& lc) {
  OStoreRetrieveRequestAccessor requestAccessor(retrieveRequest);
  insertRetrieveRequest(requestAccessor, previousOwner, copyNb, lc);
}

void Sorter::insertRetrieveRequest(SorterRetrieveRequest& retrieveRequest, AgentReferenceInterface& previousOwner, std::optional<uint32_t> copyNb, log::LogContext& lc) {
  SorterRetrieveRequestAccessor accessor(retrieveRequest);
  insertRetrieveRequest(accessor, previousOwner, copyNb, lc);
}

void Sorter::insertRetrieveRequest(RetrieveRequestInfosAccessorInterface& accessor, AgentReferenceInterface& previousOwner, std::optional<uint32_t> copyNb, log::LogContext& lc) {
  if(copyNb == std::nullopt) {
    // The job to queue is a ToTransfer job
    std::set<std::string> candidateVidsToTransfer = getCandidateVidsToTransfer(accessor);
    if(candidateVidsToTransfer.empty()) {
      throw cta::exception::Exception("In Sorter::insertRetrieveRequest(): there are no ToTransfer jobs in the RetrieveRequest and copyNb was not provided.");
    }

    std::string bestVid;
    try {
      bestVid = Helpers::selectBestRetrieveQueue(candidateVidsToTransfer, m_catalogue, m_objectstore);
    } catch(Helpers::NoTapeAvailableForRetrieve&) {
      std::stringstream err;
      err << "In Sorter::insertRetrieveRequest(): no vid available. archiveId=" << accessor.getArchiveFile().archiveFileID;
      throw RetrieveRequestHasNoCopies(err.str());
    }

    const auto& tapeFileList = accessor.getArchiveFile().tapeFiles;
    if(auto vid_it = std::find_if(begin(tapeFileList), end(tapeFileList), 
      [&bestVid](const common::dataStructures::TapeFile& tf) { return tf.vid == bestVid; });
      vid_it == std::end(tapeFileList)) {
      std::stringstream err;
      err << "In Sorter::insertRetrieveRequest(): no tape file for requested vid. archiveId="
          << accessor.getArchiveFile().archiveFileID << " vid=" << bestVid;
      throw RetrieveRequestHasNoCopies(err.str());
    }

    std::shared_ptr<RetrieveJobQueueInfo> rjqi = std::make_shared<RetrieveJobQueueInfo>(RetrieveJobQueueInfo());
    log::ScopedParamContainer params(lc);
    size_t copyNb = std::numeric_limits<size_t>::max();
    uint64_t fSeq = std::numeric_limits<uint64_t>::max();
    for(const auto& tc: accessor.getArchiveFile().tapeFiles) {
      if(tc.vid == bestVid) {
        copyNb = tc.copyNb;
        fSeq = tc.fSeq;
      }
    }
    cta::common::dataStructures::ArchiveFile archiveFile = accessor.getArchiveFile();
    try {
      Sorter::RetrieveJob jobToAdd = accessor.createRetrieveJob(archiveFile, copyNb, fSeq, &previousOwner);
      // We are sure that we want to queue a ToTransfer Job
      rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
      threading::MutexLocker mapLocker(m_mutex);
      m_retrieveQueuesAndRequests[std::make_tuple(bestVid, common::dataStructures::JobQueueType::JobsToTransferForUser)].emplace_back(rjqi);
      params.add("fileId", accessor.getArchiveFile().archiveFileID)
            .add("copyNb", copyNb)
            .add("tapeVid", bestVid)
            .add("fSeq", fSeq);
      lc.log(log::INFO, "Selected vid to be queued for retrieve request.");
      return;
    } catch(const cta::exception::Exception& ex) {
      log::ScopedParamContainer params(lc);
      params.add("fileId", accessor.getArchiveFile().archiveFileID)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In Sorter::insertRetrieveRequest() Failed to determine destination queue for retrieve request.");
      throw;
    }
  } else {
    // The job to queue is a specific job identified by its copyNb
    log::ScopedParamContainer params(lc);
    auto rjqi = std::make_shared<RetrieveJobQueueInfo>();
    cta::common::dataStructures::ArchiveFile archiveFile = accessor.getArchiveFile();
    cta::common::dataStructures::TapeFile jobTapeFile = archiveFile.tapeFiles.at(copyNb.value());
    try{
      Sorter::RetrieveJob jobToAdd = accessor.createRetrieveJob(archiveFile,jobTapeFile.copyNb,jobTapeFile.fSeq,&previousOwner);
      rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
      std::string containerId = getContainerID(accessor,jobTapeFile.vid, copyNb.value());
      threading::MutexLocker mapLocker(m_mutex);
      m_retrieveQueuesAndRequests[std::make_tuple(containerId, jobToAdd.jobQueueType)].emplace_back(rjqi);
      params.add("fileId", accessor.getArchiveFile().archiveFileID)
               .add("copyNb", copyNb.value())
               .add("tapeVid", jobTapeFile.vid)
               .add("fSeq", jobTapeFile.fSeq);
      lc.log(log::INFO, "Selected the vid of the job to be queued for retrieve request.");
    } catch(const cta::exception::Exception& ex) {
      log::ScopedParamContainer params(lc);
      params.add("fileId", accessor.getArchiveFile().archiveFileID)
            .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In Sorter::insertRetrieveRequest() Failed to determine destination queue for retrieve request.");
      throw;
    }
  }
}

std::set<std::string> Sorter::getCandidateVidsToTransfer(RetrieveRequestInfosAccessorInterface &requestAccessor){
  using serializers::RetrieveJobStatus;
  std::set<std::string> candidateVids;
  for(auto& j: requestAccessor.getJobs()){
    if(j.status == RetrieveJobStatus::RJS_ToTransfer){
      candidateVids.insert(requestAccessor.getArchiveFile().tapeFiles.at(j.copyNb).vid);
    }
  }
  return candidateVids;
}

std::string Sorter::getContainerID(RetrieveRequestInfosAccessorInterface& requestAccessor, const std::string& vid, const uint32_t copyNb){
  serializers::RetrieveJobStatus rjs = requestAccessor.getJobStatus(copyNb);
  if(rjs == serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess || rjs == serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure)
    return requestAccessor.getRepackAddress();
  return vid;
}

bool Sorter::flushOneRetrieve(log::LogContext &lc){
  threading::MutexLocker locker(m_mutex);
  for(auto & kv: m_retrieveQueuesAndRequests){
    if(!kv.second.empty()){
      queueRetrieveRequests(std::get<0>(kv.first),std::get<1>(kv.first),kv.second,lc);
      m_retrieveQueuesAndRequests.erase(kv.first);
      return true;
    }
  }
  return false;
}

Sorter::MapRetrieve Sorter::getAllRetrieve(){
  threading::MutexLocker mapLocker(m_mutex);
  return m_retrieveQueuesAndRequests;
}

void Sorter::queueRetrieveRequests(const std::string& vid, const common::dataStructures::JobQueueType& jobQueueType,
  std::list<std::shared_ptr<RetrieveJobQueueInfo>>& retrieveJobsInfos, log::LogContext &lc) {
  std::string queueAddress;
  this->dispatchRetrieveAlgorithm(vid, jobQueueType, queueAddress, retrieveJobsInfos, lc);
}

/* End of Retrieve related algorithms */

void Sorter::flushAll(log::LogContext& lc){
  while(flushOneRetrieve(lc)){}
  while(flushOneArchive(lc)){}
}

/* END OF SORTER CLASS */


/* RetrieveRequestInfosAccessor CLASS */

RetrieveRequestInfosAccessorInterface::RetrieveRequestInfosAccessorInterface(){}

RetrieveRequestInfosAccessorInterface::~RetrieveRequestInfosAccessorInterface(){}

/* END OF RetrieveRequestInfosAccessor CLASS */


/* RetrieveRequestAccessor CLASS */

OStoreRetrieveRequestAccessor::OStoreRetrieveRequestAccessor(std::shared_ptr<RetrieveRequest> retrieveRequest):m_retrieveRequest(retrieveRequest){}

OStoreRetrieveRequestAccessor::~OStoreRetrieveRequestAccessor(){}

std::list<RetrieveRequest::JobDump> OStoreRetrieveRequestAccessor::getJobs(){
  return m_retrieveRequest->dumpJobs();
}

common::dataStructures::ArchiveFile OStoreRetrieveRequestAccessor::getArchiveFile(){
  return m_retrieveRequest->getArchiveFile();
}

Sorter::RetrieveJob OStoreRetrieveRequestAccessor::createRetrieveJob(const cta::common::dataStructures::ArchiveFile& archiveFile,
        const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner){
  Sorter::RetrieveJob ret;
  ret.jobDump.copyNb = copyNb;
  ret.fSeq = fSeq;
  ret.mountPolicy = m_retrieveRequest->getRetrieveFileQueueCriteria().mountPolicy;
  ret.retrieveRequest = m_retrieveRequest;
  ret.previousOwner = previousOwner;
  ret.jobDump.status = m_retrieveRequest->getJobStatus(ret.jobDump.copyNb);
  ret.jobQueueType = m_retrieveRequest->getQueueType(copyNb);
  ret.fileSize = archiveFile.fileSize;
  ret.activity = m_retrieveRequest->getActivity();
  ret.diskSystemName = m_retrieveRequest->getDiskSystemName();
  return ret;
}

serializers::RetrieveJobStatus OStoreRetrieveRequestAccessor::getJobStatus(const uint32_t copyNb) {
  return m_retrieveRequest->getJobStatus(copyNb);
}

std::string OStoreRetrieveRequestAccessor::getRepackAddress() {
  return m_retrieveRequest->getRepackInfo().repackRequestAddress;
}

bool OStoreRetrieveRequestAccessor::getIsRepack() {
  return m_retrieveRequest->getRepackInfo().isRepack;
}

/* END OF RetrieveRequestAccessor CLASS */


/* SorterRetrieveRequestAccessor CLASS */

SorterRetrieveRequestAccessor::SorterRetrieveRequestAccessor(Sorter::SorterRetrieveRequest& request):m_retrieveRequest(request){}

SorterRetrieveRequestAccessor::~SorterRetrieveRequestAccessor() {}

std::list<RetrieveRequest::JobDump> SorterRetrieveRequestAccessor::getJobs() {
  std::list<RetrieveRequest::JobDump> ret;
  for (auto& kv : m_retrieveRequest.retrieveJobs) {
    ret.push_back(kv.second.jobDump);
  }
  return ret;
}

common::dataStructures::ArchiveFile SorterRetrieveRequestAccessor::getArchiveFile() {
  return m_retrieveRequest.archiveFile;
}

Sorter::RetrieveJob SorterRetrieveRequestAccessor::createRetrieveJob(const cta::common::dataStructures::ArchiveFile& archiveFile,
  const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner) {
  return m_retrieveRequest.retrieveJobs.at(copyNb);
}

serializers::RetrieveJobStatus SorterRetrieveRequestAccessor::getJobStatus(const uint32_t copyNb) {
  return m_retrieveRequest.retrieveJobs.at(copyNb).jobDump.status;
}

std::string SorterRetrieveRequestAccessor::getRepackAddress() {
  return m_retrieveRequest.repackRequestAddress;
}

bool SorterRetrieveRequestAccessor::getIsRepack() {
  return m_retrieveRequest.isRepack;
}

/* END OF SorterRetrieveRequestAccessor CLASS*/

} // namespace cta::objectstore
