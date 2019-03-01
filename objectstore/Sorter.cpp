/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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
#include "Sorter.hpp"
#include "Helpers.hpp"
#include "common/threading/MutexLocker.hpp"
#include <iostream>

namespace cta { namespace objectstore {
  
Sorter::Sorter(AgentReference& agentReference, Backend& objectstore, catalogue::Catalogue& catalogue):m_agentReference(agentReference),m_objectstore(objectstore),m_catalogue(catalogue){}  

Sorter::~Sorter() {
}

/* Archive related algorithms */

template <typename SpecificQueue>
void Sorter::executeArchiveAlgorithm(const std::string tapePool, std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext& lc){
  typedef ContainerAlgorithms<ArchiveQueue,SpecificQueue> Algo;
  Algo algo(m_objectstore,m_agentReference);
  typename Algo::InsertedElement::list jobsToAdd;
  std::map<uint64_t,std::shared_ptr<ArchiveJobQueueInfo>> succeededJobs;
  std::string previousOwner;
  for(auto& jobToAdd: jobs){
    Sorter::ArchiveJob job = std::get<0>(jobToAdd->jobToQueue);
    succeededJobs[job.jobDump.copyNb] = jobToAdd;
    previousOwner = job.previousOwner->getAgentAddress();
    jobsToAdd.push_back({ job.archiveRequest.get() ,job.jobDump.copyNb,job.archiveFile, job.mountPolicy,cta::nullopt });
  }
  try{
      algo.referenceAndSwitchOwnershipIfNecessary(tapePool,previousOwner,queueAddress,jobsToAdd,lc);
  } catch (typename Algo::OwnershipSwitchFailure &failure){
    for(auto &failedAR: failure.failedElements){
      try{
        std::rethrow_exception(failedAR.failure);
      } catch(cta::exception::Exception &e){
        uint32_t copyNb = failedAR.element->copyNb;
        std::get<1>(succeededJobs[copyNb]->jobToQueue).set_exception(std::current_exception());
        succeededJobs.erase(copyNb);
      }
    }
  }
  for(auto& succeededJob: succeededJobs){
    std::get<1>(succeededJob.second->jobToQueue).set_value();
  }
}

void Sorter::dispatchArchiveAlgorithm(const std::string tapePool, const JobQueueType jobQueueType, std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext &lc){
  switch(jobQueueType){
    case JobQueueType::JobsToReportToUser:
      executeArchiveAlgorithm<ArchiveQueueToReportForUser>(tapePool,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToTransferForRepack:
      executeArchiveAlgorithm<ArchiveQueueToTransferForRepack>(tapePool,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToReportToRepackForSuccess:
      executeArchiveAlgorithm<ArchiveQueueToReportToRepackForSuccess>(tapePool,queueAddress,jobs,lc); 
      break;
    case JobQueueType::JobsToReportToRepackForFailure:
      executeArchiveAlgorithm<ArchiveQueueToReportToRepackForFailure>(tapePool,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToTransferForUser:
      executeArchiveAlgorithm<ArchiveQueueToTransferForUser>(tapePool,queueAddress,jobs,lc);
      break;
    case JobQueueType::FailedJobs:
      executeArchiveAlgorithm<ArchiveQueueFailed>(tapePool,queueAddress,jobs,lc);
      break;
    default:
      throw cta::exception::Exception("In Sorter::dispatchArchiveAlgorithm(), unknown JobQueueType");
  }
}

void Sorter::insertArchiveRequest(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface& previousOwner, log::LogContext& lc){
  for(auto& job: archiveRequest->dumpJobs()){
    insertArchiveJob(archiveRequest,previousOwner,job,lc);
  }
}

void Sorter::insertArchiveJob(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface &previousOwner, ArchiveRequest::JobDump& jobToInsert, log::LogContext & lc){
  auto ajqi = std::make_shared<ArchiveJobQueueInfo>();
  Sorter::ArchiveJob jobToAdd;
  jobToAdd.archiveRequest = archiveRequest;
  jobToAdd.archiveFile = archiveRequest->getArchiveFile();
  jobToAdd.jobDump.copyNb = jobToInsert.copyNb;
  jobToAdd.mountPolicy = archiveRequest->getMountPolicy();
  jobToAdd.previousOwner = &previousOwner;
  jobToAdd.jobDump.tapePool = jobToInsert.tapePool;
  ajqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
  try{
    threading::MutexLocker mapLocker(m_mutex);
    m_archiveQueuesAndRequests[std::make_tuple(jobToInsert.tapePool, archiveRequest->getJobQueueType(jobToInsert.copyNb))].emplace_back(ajqi);
  } catch (cta::exception::Exception &ex){
    log::ScopedParamContainer params(lc);
    params.add("fileId", archiveRequest->getArchiveFile().archiveFileID)
          .add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR,"In Sorter::insertArchiveJob() Failed to determine destination queue for Archive Job.");
  }
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

void Sorter::queueArchiveRequests(const std::string tapePool, const JobQueueType jobQueueType, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& archiveJobsInfos, log::LogContext &lc){
  std::string queueAddress;
  this->dispatchArchiveAlgorithm(tapePool,jobQueueType,queueAddress,archiveJobsInfos,lc);
}

/* End of archive related algorithms */

/* Retrieve related algorithms */

template <typename SpecificQueue>
void Sorter::executeRetrieveAlgorithm(const std::string vid, std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& jobs, log::LogContext& lc){
  typedef ContainerAlgorithms<RetrieveQueue,SpecificQueue> Algo;
  Algo algo(m_objectstore,m_agentReference);
  typename Algo::InsertedElement::list jobsToAdd;
  std::map<uint64_t, std::shared_ptr<RetrieveJobQueueInfo>> succeededJobs;
  std::string previousOwner;
  for(auto& jobToAdd: jobs){
    Sorter::RetrieveJob job = std::get<0>(jobToAdd->jobToQueue);
    succeededJobs[job.jobDump.copyNb] = jobToAdd;
    previousOwner = job.previousOwner->getAgentAddress();
    jobsToAdd.push_back({job.retrieveRequest.get(),job.jobDump.copyNb,job.fSeq,job.fileSize,job.mountPolicy,job.jobDump.status});
  }
  try{
    algo.referenceAndSwitchOwnershipIfNecessary(vid,previousOwner,queueAddress,jobsToAdd,lc);
  } catch(typename Algo::OwnershipSwitchFailure &failure){
    for(auto& failedRR: failure.failedElements){
      try{
        std::rethrow_exception(failedRR.failure);
      } catch (cta::exception::Exception){
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

void Sorter::dispatchRetrieveAlgorithm(const std::string vid, const JobQueueType jobQueueType, std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo> >& jobs, log::LogContext& lc){
  switch(jobQueueType){
    case JobQueueType::JobsToReportToUser:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportForUser>(vid,queueAddress,jobs,lc);
    break;
    case JobQueueType::JobsToTransferForUser:
      this->executeRetrieveAlgorithm<RetrieveQueueToTransferForUser>(vid,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToReportToRepackForSuccess:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportToRepackForSuccess>(vid,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToReportToRepackForFailure:
      this->executeRetrieveAlgorithm<RetrieveQueueToReportToRepackForFailure>(vid,queueAddress,jobs,lc);
      break;
    case JobQueueType::JobsToTransferForRepack:
      this->executeRetrieveAlgorithm<RetrieveQueueToTransferForRepack>(vid, queueAddress,jobs,lc);
      break;
    case JobQueueType::FailedJobs:
      break;
    default:
      throw cta::exception::Exception("In Sorter::dispatchRetrieveAlgorithm(), unknown JobQueueType");
    break;
  }
}

Sorter::RetrieveJob Sorter::createRetrieveJob(std::shared_ptr<RetrieveRequest> retrieveRequest, const cta::common::dataStructures::ArchiveFile archiveFile,
        const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner){
  
  Sorter::RetrieveJob jobToAdd;
  jobToAdd.jobDump.copyNb = copyNb;
  jobToAdd.fSeq = fSeq;
  jobToAdd.mountPolicy = retrieveRequest->getRetrieveFileQueueCriteria().mountPolicy;
  jobToAdd.retrieveRequest = retrieveRequest;
  jobToAdd.previousOwner = previousOwner;
  jobToAdd.jobDump.status = retrieveRequest->getJobStatus(jobToAdd.jobDump.copyNb);
  jobToAdd.fileSize = archiveFile.fileSize;
  return jobToAdd;
}

void Sorter::insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, AgentReferenceInterface &previousOwner, cta::optional<uint32_t> copyNb, log::LogContext & lc){
  if(copyNb == cta::nullopt){
    //The job to queue will be a ToTransfer
    std::set<std::string> candidateVidsToTransfer = getCandidateVidsToTransfer(*retrieveRequest);
    
    if(!candidateVidsToTransfer.empty()){

      std::string bestVid = getBestVidForQueueingRetrieveRequest(*retrieveRequest, candidateVidsToTransfer ,lc);

      for (auto & tf: retrieveRequest->getArchiveFile().tapeFiles) {
        if (tf.second.vid == bestVid) {
          goto vidFound;
        }
      }
      {
        std::stringstream err;
        err << "In Sorter::insertRetrieveRequest(): no tape file for requested vid. archiveId=" << retrieveRequest->getArchiveFile().archiveFileID
            << " vid=" << bestVid;
        throw RetrieveRequestHasNoCopies(err.str());
      }
      vidFound:
        std::shared_ptr<RetrieveJobQueueInfo> rjqi = std::make_shared<RetrieveJobQueueInfo>(RetrieveJobQueueInfo());
        log::ScopedParamContainer params(lc);
        size_t copyNb = std::numeric_limits<size_t>::max();
        uint64_t fSeq = std::numeric_limits<uint64_t>::max();
        for (auto & tc: retrieveRequest->getArchiveFile().tapeFiles) { if (tc.second.vid==bestVid) { copyNb=tc.first; fSeq=tc.second.fSeq; } }
        cta::common::dataStructures::ArchiveFile archiveFile = retrieveRequest->getArchiveFile();
        Sorter::RetrieveJob jobToAdd = createRetrieveJob(retrieveRequest,archiveFile,copyNb,fSeq,&previousOwner);
        //We are sure that we want to queue a ToTransfer Job
        rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
        threading::MutexLocker mapLocker(m_mutex);
        m_retrieveQueuesAndRequests[std::make_tuple(bestVid,JobQueueType::JobsToTransferForUser)].emplace_back(rjqi);
        params.add("fileId", retrieveRequest->getArchiveFile().archiveFileID)
               .add("copyNb", copyNb)
               .add("tapeVid", bestVid)
               .add("fSeq", fSeq);
        lc.log(log::INFO, "Selected vid to be queued for retrieve request.");
        return;
    } else {
      throw cta::exception::Exception("In Sorter::insertRetrieveRequest(), there is no ToTransfer jobs in the RetrieveRequest. Please provide the copyNb of the job you want to queue.");
    }
  } else {
    //We want to queue a specific job identified by its copyNb
    log::ScopedParamContainer params(lc);
    auto rjqi = std::make_shared<RetrieveJobQueueInfo>();
    cta::common::dataStructures::ArchiveFile archiveFile = retrieveRequest->getArchiveFile();
    cta::common::dataStructures::TapeFile jobTapeFile = archiveFile.tapeFiles[copyNb.value()];
    Sorter::RetrieveJob jobToAdd = createRetrieveJob(retrieveRequest,archiveFile,jobTapeFile.copyNb,jobTapeFile.fSeq,&previousOwner);
    rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
    try{
      threading::MutexLocker mapLocker(m_mutex);
      m_retrieveQueuesAndRequests[std::make_tuple(jobTapeFile.vid, retrieveRequest->getQueueType(copyNb.value()))].emplace_back(rjqi);
      params.add("fileId", retrieveRequest->getArchiveFile().archiveFileID)
               .add("copyNb", copyNb.value())
               .add("tapeVid", jobTapeFile.vid)
               .add("fSeq", jobTapeFile.fSeq);
      lc.log(log::INFO, "Selected the vid of the job to be queued for retrieve request.");
    } catch (cta::exception::Exception &ex){
      log::ScopedParamContainer params(lc);
      params.add("fileId", retrieveRequest->getArchiveFile().archiveFileID)
             .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In Sorter::insertRetrieveRequest() Failed to determine destination queue for retrieve request.");
      throw ex;
    }
  }
}

std::set<std::string> Sorter::getCandidateVidsToTransfer(RetrieveRequest &request){
  using serializers::RetrieveJobStatus;
  std::set<std::string> candidateVids;
  for (auto & j: request.dumpJobs()) {
    if(j.status == RetrieveJobStatus::RJS_ToTransferForUser) {
      candidateVids.insert(request.getArchiveFile().tapeFiles.at(j.copyNb).vid);
    }
  }
  return candidateVids;
}

std::string Sorter::getBestVidForQueueingRetrieveRequest(RetrieveRequest& retrieveRequest, std::set<std::string>& candidateVids, log::LogContext &lc){
  std::string vid;
  try{
    vid = Helpers::selectBestRetrieveQueue(candidateVids,m_catalogue,m_objectstore);
  } catch (Helpers::NoTapeAvailableForRetrieve & ex) {
    log::ScopedParamContainer params(lc);
    params.add("fileId", retrieveRequest.getArchiveFile().archiveFileID);
    lc.log(log::INFO, "In Sorter::getVidForQueueingRetrieveRequest(): No available tape found.");
    throw ex;
  }
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

void Sorter::queueRetrieveRequests(const std::string vid, const JobQueueType jobQueueType, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& retrieveJobsInfos, log::LogContext &lc){
  std::string queueAddress;
  this->dispatchRetrieveAlgorithm(vid,jobQueueType,queueAddress,retrieveJobsInfos,lc);
}

/* End of Retrieve related algorithms */

void Sorter::flushAll(log::LogContext& lc){
  while(flushOneRetrieve(lc)){}
  while(flushOneArchive(lc)){}
}

}}
