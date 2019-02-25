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
    //TODO, change the ownership by passing the previousOwner to this sorter
    previousOwner = job.previousOwner->getAgentAddress();
    jobsToAdd.push_back({ jobToAdd->archiveRequest.get(),job.jobDump.copyNb,job.archiveFile, job.mountPolicy,cta::nullopt });
  }
  try{
      algo.referenceAndSwitchOwnershipIfNecessary(tapePool,previousOwner,queueAddress,jobsToAdd,lc);
  } catch (typename Algo::OwnershipSwitchFailure &failure){
    for(auto &failedAR: failure.failedElements){
      try{
        std::rethrow_exception(failedAR.failure);
      } catch(cta::exception::Exception &e){
        uint16_t copyNb = failedAR.element->copyNb;
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
      executeArchiveAlgorithm<ArchiveQueueToReport>(tapePool,queueAddress,jobs,lc);
      break;
    default:
      executeArchiveAlgorithm<ArchiveQueueToTransfer>(tapePool,queueAddress,jobs,lc);
      break;
  }
}

void Sorter::insertArchiveJob(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface &previousOwner, ArchiveRequest::JobDump& jobToInsert, log::LogContext & lc){
  auto ajqi = std::make_shared<ArchiveJobQueueInfo>();
  ajqi->archiveRequest = archiveRequest;
  Sorter::ArchiveJob jobToAdd;
  jobToAdd.archiveRequest = archiveRequest;
  jobToAdd.archiveFile = archiveRequest->getArchiveFile();
  jobToAdd.archiveFileId = archiveRequest->getArchiveFile().archiveFileID;
  jobToAdd.jobDump.copyNb = jobToInsert.copyNb;
  jobToAdd.fileSize = archiveRequest->getArchiveFile().fileSize;
  jobToAdd.mountPolicy = archiveRequest->getMountPolicy();
  jobToAdd.previousOwner = &previousOwner;
  jobToAdd.startTime = archiveRequest->getEntryLog().time;
  jobToAdd.jobDump.tapePool = jobToInsert.tapePool;
  ajqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
  threading::MutexLocker mapLocker(m_mutex);
  m_archiveQueuesAndRequests[std::make_tuple(jobToInsert.tapePool, archiveRequest->getJobQueueType(jobToInsert.copyNb))].emplace_back(ajqi);
}

void Sorter::insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, log::LogContext & lc){
  std::set<std::string> candidateVids = getCandidateVids(*retrieveRequest);
  //We need to select the best VID to queue the RetrieveJob in the best queue
  if(candidateVids.empty()){
    auto rjqi = std::make_shared<RetrieveJobQueueInfo>();
    rjqi->retrieveRequest = retrieveRequest;
    //The first copy of the ArchiveFile will be queued
    cta::common::dataStructures::TapeFile jobTapeFile = retrieveRequest->getArchiveFile().tapeFiles.begin()->second;
    Sorter::RetrieveJob jobToAdd;
    jobToAdd.jobDump.copyNb = jobTapeFile.copyNb;
    jobToAdd.fSeq = jobTapeFile.fSeq;
    jobToAdd.fileSize = retrieveRequest->getArchiveFile().fileSize;
    jobToAdd.mountPolicy = retrieveRequest->getRetrieveFileQueueCriteria().mountPolicy;
    jobToAdd.retrieveRequest = retrieveRequest;
    jobToAdd.startTime = retrieveRequest->getEntryLog().time;
    rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
    try{
      threading::MutexLocker mapLocker(m_mutex);
      m_retrieveQueuesAndRequests[std::make_tuple(retrieveRequest->getArchiveFile().tapeFiles.begin()->second.vid, retrieveRequest->getQueueType())].emplace_back(rjqi);
    } catch (cta::exception::Exception &ex){
      log::ScopedParamContainer params(lc);
      params.add("fileId", retrieveRequest->getArchiveFile().archiveFileID)
             .add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "In Sorter::insertRetrieveRequest() Failed to determine destination queue for retrieve request.");
      throw ex;
    }
  }
  std::string bestVid = getBestVidForQueueingRetrieveRequest(*retrieveRequest, candidateVids ,lc);
  
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
    rjqi->retrieveRequest = retrieveRequest;
    log::ScopedParamContainer params(lc);
    size_t copyNb = std::numeric_limits<size_t>::max();
    uint64_t fSeq = std::numeric_limits<uint64_t>::max();
    for (auto & tc: retrieveRequest->getArchiveFile().tapeFiles) { if (tc.second.vid==bestVid) { copyNb=tc.first; fSeq=tc.second.fSeq; } }
    Sorter::RetrieveJob jobToAdd;
    jobToAdd.jobDump.copyNb = copyNb;
    jobToAdd.fSeq = fSeq;
    jobToAdd.fileSize = retrieveRequest->getArchiveFile().fileSize;
    jobToAdd.mountPolicy = retrieveRequest->getRetrieveFileQueueCriteria().mountPolicy;
    jobToAdd.retrieveRequest = retrieveRequest;
    jobToAdd.startTime = retrieveRequest->getEntryLog().time;
    
    threading::MutexLocker mapLocker(m_mutex);
    //We are sure that we want to transfer jobs
    rjqi->jobToQueue = std::make_tuple(jobToAdd,std::promise<void>());
    m_retrieveQueuesAndRequests[std::make_tuple(bestVid,JobQueueType::JobsToTransfer)].emplace_back(rjqi);
    params.add("fileId", retrieveRequest->getArchiveFile().archiveFileID)
           .add("copyNb", copyNb)
           .add("tapeVid", bestVid)
           .add("fSeq", fSeq);
    lc.log(log::INFO, "Selected vid to be queued for retrieve request.");
}

std::set<std::string> Sorter::getCandidateVids(RetrieveRequest &request){
  using serializers::RetrieveJobStatus;
  std::set<std::string> candidateVids;
  for (auto & j: request.dumpJobs()) {
    if(j.status == RetrieveJobStatus::RJS_ToTransfer) {
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

bool Sorter::flushOneArchive(log::LogContext &lc) {
  threading::MutexLocker locker(m_mutex);
  for(auto & kv: m_archiveQueuesAndRequests){
    if(!kv.second.empty()){
      queueArchiveRequests(std::get<0>(kv.first),std::get<1>(kv.first),kv.second,lc);
      return true;
    }
  }
  return false;
}

bool Sorter::flushOneRetrieve(log::LogContext &lc){
  return true;
}

Sorter::MapArchive Sorter::getAllArchive(){
  return m_archiveQueuesAndRequests;
}

Sorter::MapRetrieve Sorter::getAllRetrieve(){
  return m_retrieveQueuesAndRequests;
}

void Sorter::queueArchiveRequests(const std::string tapePool, const JobQueueType jobQueueType, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& archiveJobInfos, log::LogContext &lc){

   /* double queueLockFetchTime=0;
    double queueProcessAndCommitTime=0;
    double requestsUpdatePreparationTime=0;
    double requestsUpdatingTime = 0;
    utils::Timer t;*/
    /*uint64_t filesBefore=0;
    uint64_t bytesBefore=0;*/
    
    /*ArchiveQueue aq(m_objectstore);
    ScopedExclusiveLock rql;
    Helpers::getLockedAndFetchedJobQueue<ArchiveQueue>(aq,rql, m_agentReference, tapePool, jobQueueType, lc);
    queueLockFetchTime = t.secs(utils::Timer::resetCounter);
    auto jobsSummary=aq.getJobsSummary();
    filesBefore=jobsSummary.jobs;
    bytesBefore=jobsSummary.bytes;
    // Prepare the list of requests to add to the queue (if needed).
    std::list<ArchiveQueue::JobToAdd> jta;
    // We have the queue. We will loop on the requests, add them to the list. We will launch their updates 
    // after committing the queue.
    Sorter::ArchiveJob jobToQueue = std::get<0>(archiveJobInfo->jobToQueue);
    std::promise<void>& jobPromise = std::get<1>(archiveJobInfo->jobToQueue);
    
    jta.push_back({jobToQueue.jobDump,jobToQueue.archiveRequest->getAddressIfSet(),jobToQueue.archiveFileId,jobToQueue.fileSize,jobToQueue.mountPolicy,jobToQueue.startTime});
    auto addedJobs = aq.addJobsIfNecessaryAndCommit(jta,m_agentReference,lc);
    queueProcessAndCommitTime = t.secs(utils::Timer::resetCounter);
    if(!addedJobs.files){
      try{
        throw cta::exception::Exception("In Sorter::queueArchiveRequests, no job have been added with addJobsIfNecessaryAndCommit().");
      } catch (cta::exception::Exception &e){
        jobPromise.set_exception(std::current_exception());
        continue;
      }
    }
    // We will keep individual references for the job update we launch so that we make
    // our life easier downstream.
    struct ARUpdatedParams {
      std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater> updater;
      std::shared_ptr<ArchiveRequest> archiveRequest;
      uint16_t copyNb;
    };
    
    ARUpdatedParams arUpdaterParams;
    arUpdaterParams.archiveRequest = archiveJobInfo->archiveRequest;
    arUpdaterParams.copyNb = jobToQueue.jobDump.copyNb;
    //Here the new owner is the agentReference of the process that runs the sorter, ArchiveRequest has no owner, the jobs have
    arUpdaterParams.updater.reset(archiveJobInfo->archiveRequest->asyncUpdateJobOwner(jobToQueue.jobDump.copyNb,m_agentReference.getAgentAddress(),jobToQueue.jobDump.owner,cta::nullopt));
    
    requestsUpdatePreparationTime = t.secs(utils::Timer::resetCounter);
    try{
      arUpdaterParams.updater->wait();
      //No problem, the job has been inserted into the queue, log it.
      jobPromise.set_value();
      log::ScopedParamContainer params(lc);
      params.add("archiveRequestObject", archiveJobInfo->archiveRequest->getAddressIfSet())
            .add("copyNb", arUpdaterParams.copyNb)
            .add("fileId",arUpdaterParams.updater->getArchiveFile().archiveFileID)
            .add("tapePool",tapePool)
            .add("archiveQueueObject",aq.getAddressIfSet())
            .add("previousOwner",jobToQueue.jobDump.owner);
      lc.log(log::INFO, "In Sorter::queueArchiveRequests(): queued archive job.");
    } catch (cta::exception::Exception &e){
      jobPromise.set_exception(std::current_exception());
      continue;
    }
    */
    std::string queueAddress;
    this->dispatchArchiveAlgorithm(tapePool,jobQueueType,queueAddress,archiveJobInfos,lc);
    archiveJobInfos.clear();
    /*requestsUpdatingTime = t.secs(utils::Timer::resetCounter);
    {
      log::ScopedParamContainer params(lc);
      ArchiveQueue aq(queueAddress,m_objectstore);
      ScopedExclusiveLock aql(aq);
      aq.fetch();
      auto jobsSummary = aq.getJobsSummary();
      params.add("tapePool", tapePool)
            .add("archiveQueueObject", aq.getAddressIfSet())
            .add("filesAdded", filesQueued)
            .add("bytesAdded", bytesQueued)
            .add("filesAddedInitially", filesQueued)
            .add("bytesAddedInitially", bytesQueued)
            .add("filesDequeuedAfterErrors", filesDequeued)
            .add("bytesDequeuedAfterErrors", bytesDequeued)
            .add("filesBefore", filesBefore)
            .add("bytesBefore", bytesBefore)
            .add("filesAfter", jobsSummary.jobs)
            .add("bytesAfter", jobsSummary.bytes)
            .add("queueLockFetchTime", queueLockFetchTime)
            .add("queuePreparationTime", queueProcessAndCommitTime)
            .add("requestsUpdatePreparationTime", requestsUpdatePreparationTime)
            .add("requestsUpdatingTime", requestsUpdatingTime);
            //.add("queueRecommitTime", queueRecommitTime);
      lc.log(log::INFO, "In Sorter::queueArchiveRequests(): "
          "Queued an archiveRequest");
    }*/
}

void Sorter::queueRetrieveRequests(const std::string vid, const JobQueueType jobQueueType, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& retrieveJobsInfo, log::LogContext &lc){
  /*for(auto& retrieveJobInfo: retrieveJobsInfo){
    double queueLockFetchTime=0;
    double queueProcessAndCommitTime=0;
    //double requestsUpdatePreparationTime=0;
    //double requestsUpdatingTime = 0;
    utils::Timer t;
    uint64_t filesBefore=0;
    uint64_t bytesBefore=0;
    
    RetrieveQueue rq(m_objectstore);
    ScopedExclusiveLock rql;
    Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq,rql,m_agentReference,vid,jobQueueType,lc);
    queueLockFetchTime = t.secs(utils::Timer::resetCounter);
    auto jobsSummary=rq.getJobsSummary();
    filesBefore=jobsSummary.jobs;
    bytesBefore=jobsSummary.bytes;
    
    Sorter::RetrieveJob jobToQueue = std::get<0>(retrieveJobInfo->jobToQueue);
    std::promise<void>& jobPromise = std::get<1>(retrieveJobInfo->jobToQueue);
    
    std::list<RetrieveQueue::JobToAdd> jta;
    jta.push_back({jobToQueue.jobDump.copyNb,jobToQueue.fSeq,jobToQueue.retrieveRequest->getAddressIfSet(),jobToQueue.fileSize,jobToQueue.mountPolicy,jobToQueue.startTime});
    
    auto addedJobs = rq.addJobsIfNecessaryAndCommit(jta, m_agentReference, lc);
    queueProcessAndCommitTime = t.secs(utils::Timer::resetCounter);
    
    if(!addedJobs.files){
      throw cta::exception::Exception("In Sorter::queueRetrieveRequests(), failed of adding a job to the retrieve queue through addJobsIfNecessaryAndCommit()");
    }
    
    // We will keep individual references for each job update we launch so that we make
    // our life easier downstream.
    struct RRUpdatedParams {
      std::unique_ptr<RetrieveRequest::AsyncJobOwnerUpdater> updater;
      std::shared_ptr<RetrieveRequest> retrieveRequest;
      uint64_t copyNb;
    };
    
    {
      std::list<RRUpdatedParams> rrUpdatersParams;
      
    }
    
    if(queueLockFetchTime && filesBefore && bytesBefore &&queueProcessAndCommitTime){}
    
  }*/
}

}}
