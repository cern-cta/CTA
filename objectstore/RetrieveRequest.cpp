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

#include "RetrieveRequest.hpp"
#include "GenericObject.hpp"
#include "EntryLogSerDeser.hpp"
#include "MountPolicySerDeser.hpp"
#include "DiskFileInfoSerDeser.hpp"
#include "ArchiveFileSerDeser.hpp"
#include "RetrieveQueue.hpp"
#include "objectstore/cta.pb.h"
#include "Helpers.hpp"
#include "common/utils/utils.hpp"
#include <google/protobuf/util/json_util.h>
#include <cmath>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RetrieveRequest::RetrieveRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(os, address) { }

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RetrieveRequest::RetrieveRequest(GenericObject& go):
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

//------------------------------------------------------------------------------
// RetrieveRequest::initialize()
//------------------------------------------------------------------------------
void RetrieveRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>::initialize();
  m_payload.set_failurereportlog("");
  m_payload.set_failurereporturl("");
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// RetrieveRequest::garbageCollect()
//------------------------------------------------------------------------------
void RetrieveRequest::garbageCollect(const std::string& presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  utils::Timer t;
  // Check the request is indeed owned by the right owner.
  if (getOwner() != presumedOwner) {
    log::ScopedParamContainer params(lc);
    params.add("jobObject", getAddressIfSet())
          .add("presumedOwner", presumedOwner)
          .add("owner", getOwner());
    lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): no garbage collection needed.");
  }
  // The owner is indeed the right one. We should requeue the request either to
  // the to tranfer queue for one vid, or to the to report (or failed) queue (for one arbitrary VID).
  // Find the vids for active jobs in the request (to transfer ones).
  using serializers::RetrieveJobStatus;
  std::set<std::string> candidateVids;
  for (auto &j: m_payload.jobs()) {
    if (j.status() == RetrieveJobStatus::RJS_ToTransfer) {
      // Find the job details in tape file
      for (auto &tf: m_payload.archivefile().tapefiles()) {
        if (tf.copynb() == j.copynb()) {
          candidateVids.insert(tf.vid());
          goto found;
        }
      }
      {
        std::stringstream err;
        err << "In RetrieveRequest::garbageCollect(): could not find tapefile for copynb " << j.copynb();
        throw exception::Exception(err.str());
      }
    found:;
    }
  }
  std::string bestVid;
  // If no tape file is a candidate, we just need to skip to queueing to the failed queue
  if (candidateVids.empty()) goto queueForFailure;
  // We have a chance to find an available tape. Let's compute best VID (this will
  // filter on tape availability.
  try {
    // If we have to fetch the status of the tapes and queued for the non-disabled vids.
    auto bestVid=Helpers::selectBestRetrieveQueue(candidateVids, catalogue, m_objectStore);
    goto queueForTransfer;
  } catch (Helpers::NoTapeAvailableForRetrieve &) {}
queueForFailure:;
  {
    // If there is no candidate, we fail the jobs that are not yet, and queue the request as failed (on any VID).
    for (auto & j: *m_payload.mutable_jobs()) {
      if (j.status() == RetrieveJobStatus::RJS_ToTransfer) {
        j.set_status(RetrieveJobStatus::RJS_Failed);
    log::ScopedParamContainer params(lc);
        params.add("fileId", m_payload.archivefile().archivefileid())
              .add("copyNb", j.copynb());
        for (auto &tf: m_payload.archivefile().tapefiles()) {
          if (tf.copynb() == j.copynb()) {
            params.add("vid", tf.vid())
                  .add("fSeq", tf.fseq());
            break;
          }
        }
        // Generate the last failure for this job (tape unavailable).
        *j.mutable_failurelogs()->Add() = utils::getCurrentLocalTime() + " " + 
            utils::getShortHostname() + " In RetrieveRequest::garbageCollect(): No VID avaiable to requeue the request. Failing it.";
        lc.log(log::ERR, "In RetrieveRequest::garbageCollect(): No VID avaiable to requeue the request. Failing all jobs.");
      }
    }
    // Ok, the request is ready to be queued. We will queue it to the VID corresponding
    // to the latest queued copy.
    auto activeCopyNb = m_payload.activecopynb();
    std::string activeVid;
    uint64_t activeFseq;
    for (auto &tf: m_payload.archivefile().tapefiles()) {
      if (tf.copynb() == activeCopyNb) {
        activeVid = tf.vid();
        activeFseq = tf.fseq();
        goto failedVidFound;
      }
    }
    {
      std::stringstream err;
      err << "In RetrieveRequest::garbageCollect(): could not find tapefile for copynb " << activeCopyNb;
      throw exception::Exception(err.str());
    }
  failedVidFound:;
    // We now need to grab the failed queue and queue the request.
    RetrieveQueue rq(m_objectStore);
    ScopedExclusiveLock rql;
    Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq, rql, agentReference, bestVid, JobQueueType::JobsToReport, lc);
    // Enqueue the job
    objectstore::MountPolicySerDeser mp;
    std::list<RetrieveQueue::JobToAdd> jta;
    jta.push_back({activeCopyNb, activeFseq, getAddressIfSet(), m_payload.archivefile().filesize(), 
      mp, (signed)m_payload.schedulerrequest().entrylog().time()});
    rq.addJobsIfNecessaryAndCommit(jta, agentReference, lc);
    auto queueUpdateTime = t.secs(utils::Timer::resetCounter);
    // We can now make the transition official.
    setOwner(rq.getAddressIfSet());
    commit();
    rql.release();
    auto commitUnlockQueueTime = t.secs(utils::Timer::resetCounter);
    {
      log::ScopedParamContainer params(lc);
      params.add("jobObject", getAddressIfSet())
            .add("fileId", m_payload.archivefile().archivefileid())
            .add("queueObject", rq.getAddressIfSet())
            .add("copynb", activeCopyNb)
            .add("vid", activeVid)
            .add("queueUpdateTime", queueUpdateTime)
            .add("commitUnlockQueueTime", commitUnlockQueueTime);
      lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): queued the request to the failed queue.");
    }
  }
  
  // Find the corresponding tape file, which will give the copynb, which will allow finding the retrieve job.
queueForTransfer:;
  {
    auto bestTapeFile=m_payload.archivefile().tapefiles().begin();
    while (bestTapeFile != m_payload.archivefile().tapefiles().end()) {
      if (bestTapeFile->vid() == bestVid)
        goto tapeFileFound;
      bestTapeFile++;
    }
    {
      std::stringstream err;
      err << "In RetrieveRequest::garbageCollect(): could not find tapefile for vid " << bestVid;
      throw exception::Exception(err.str());
    }
  tapeFileFound:;
    auto tapeSelectionTime = t.secs(utils::Timer::resetCounter);
    auto bestJob=m_payload.mutable_jobs()->begin();
    while (bestJob!=m_payload.mutable_jobs()->end()) {
      if (bestJob->copynb() == bestTapeFile->copynb())
        goto jobFound;
      bestJob++;
    }
    {
      std::stringstream err;
      err << "In RetrieveRequest::garbageCollect(): could not find job for copynb " << bestTapeFile->copynb();
      throw exception::Exception(err.str());
    }
  jobFound:;
      // We now need to grab the queue and requeue the request.
    RetrieveQueue rq(m_objectStore);
    ScopedExclusiveLock rql;
      Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq, rql, agentReference, bestVid, JobQueueType::JobsToTransfer, lc);
      // Enqueue the job
    objectstore::MountPolicySerDeser mp;
    mp.deserialize(m_payload.mountpolicy());
    std::list<RetrieveQueue::JobToAdd> jta;
    jta.push_back({bestTapeFile->copynb(), bestTapeFile->fseq(), getAddressIfSet(), m_payload.archivefile().filesize(), 
      mp, (signed)m_payload.schedulerrequest().entrylog().time()});
    rq.addJobsIfNecessaryAndCommit(jta, agentReference, lc);
    auto jobsSummary=rq.getJobsSummary();
    auto queueUpdateTime = t.secs(utils::Timer::resetCounter);
    // We can now make the transition official.
    m_payload.set_activecopynb(bestJob->copynb());
    setOwner(rq.getAddressIfSet());
    commit();
    Helpers::updateRetrieveQueueStatisticsCache(bestVid, jobsSummary.files, jobsSummary.bytes, jobsSummary.priority);
    rql.release();
    auto commitUnlockQueueTime = t.secs(utils::Timer::resetCounter);
    {
      log::ScopedParamContainer params(lc);
      params.add("jobObject", getAddressIfSet())
            .add("fileId", m_payload.archivefile().archivefileid())
            .add("queueObject", rq.getAddressIfSet())
            .add("copynb", bestTapeFile->copynb())
            .add("vid", bestTapeFile->vid())
            .add("tapeSelectionTime", tapeSelectionTime)
            .add("queueUpdateTime", queueUpdateTime)
            .add("commitUnlockQueueTime", commitUnlockQueueTime);
      lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): requeued the request.");
    }
    timespec ts;
    // We will sleep a bit to make sure other processes can also access the queue
    // as we are very likely to be part of a tight loop.
    // TODO: ideally, end of session requeueing and garbage collection should be
    // done in parallel.
    // We sleep half the time it took to queue to give a chance to other lockers.
    double secSleep, fracSecSleep;
    fracSecSleep = std::modf(queueUpdateTime / 2, &secSleep);
    ts.tv_sec = secSleep;
    ts.tv_nsec = std::round(fracSecSleep * 1000 * 1000 * 1000);
    nanosleep(&ts, nullptr);
    auto sleepTime = t.secs();
    {
      log::ScopedParamContainer params(lc);
      params.add("jobObject", getAddressIfSet())
            .add("fileId", m_payload.archivefile().archivefileid())
            .add("queueObject", rq.getAddressIfSet())
            .add("copynb", bestTapeFile->copynb())
            .add("vid", bestTapeFile->vid())
            .add("tapeSelectionTime", tapeSelectionTime)
            .add("queueUpdateTime", queueUpdateTime)
            .add("commitUnlockQueueTime", commitUnlockQueueTime)
            .add("sleepTime", sleepTime);
      lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): slept some time to not sit on the queue after GC requeueing.");
    }
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::addJob()
//------------------------------------------------------------------------------
void RetrieveRequest::addJob(uint64_t copyNb, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries,
  uint16_t maxReportRetries)
{
  checkPayloadWritable();
  auto *tf = m_payload.add_jobs();
  tf->set_copynb(copyNb);
  tf->set_lastmountwithfailure(0);
  tf->set_maxretrieswithinmount(maxRetriesWithinMount);
  tf->set_maxtotalretries(maxTotalRetries);
  tf->set_retrieswithinmount(0);
  tf->set_totalretries(0);
  tf->set_maxreportretries(maxReportRetries);
  tf->set_totalreportretries(0);
  tf->set_status(serializers::RetrieveJobStatus::RJS_ToTransfer);
}

//------------------------------------------------------------------------------
// addTransferFailure()
//------------------------------------------------------------------------------
auto RetrieveRequest::addTransferFailure(uint16_t copyNumber, uint64_t mountId, const std::string &failureReason,
  log::LogContext &lc) -> EnqueueingNextStep
{
  checkPayloadWritable();

  // Find the job and update the number of failures
  for(int i = 0; i < m_payload.jobs_size(); i++) {
    auto &j = *m_payload.mutable_jobs(i);

    if(j.copynb() == copyNumber) {
      if(j.lastmountwithfailure() == mountId) {
        j.set_retrieswithinmount(j.retrieswithinmount() + 1);
      } else {
        j.set_retrieswithinmount(1);
        j.set_lastmountwithfailure(mountId);
      }
      j.set_totalretries(j.totalretries() + 1);
      *j.mutable_failurelogs()->Add() = failureReason;
    }

    if(j.totalretries() < j.maxtotalretries()) {
      EnqueueingNextStep ret;
      ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToTransfer;
      if(j.retrieswithinmount() < j.maxretrieswithinmount())
        // Job can try again within this mount
        ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForTransfer;
      else
        // No more retries within this mount: job remains owned by this session and will be garbage collected
        ret.nextStep = EnqueueingNextStep::NextStep::Nothing;
      return ret;
    } else {
      // All retries within all mounts have been exhausted
      return determineNextStep(copyNumber, JobEvent::TransferFailed, lc);
    }
  }
  throw NoSuchJob("In RetrieveRequest::addJobFailure(): could not find job");
}

//------------------------------------------------------------------------------
// addReportFailure()
//------------------------------------------------------------------------------
auto RetrieveRequest::addReportFailure(uint16_t copyNumber, uint64_t sessionId, const std::string &failureReason,
  log::LogContext &lc) -> EnqueueingNextStep
{
  checkPayloadWritable();
  // Find the job and update the number of failures
  for(int i = 0; i < m_payload.jobs_size(); ++i)
  {
    auto &j = *m_payload.mutable_jobs(i);
    if (j.copynb() == copyNumber) {
      j.set_totalreportretries(j.totalreportretries() + 1);
      * j.mutable_reportfailurelogs()->Add() = failureReason;
    }
    EnqueueingNextStep ret;
    if (j.totalreportretries() >= j.maxreportretries()) {
      // Status is now failed
      ret.nextStatus = serializers::RetrieveJobStatus::RJS_Failed;
      ret.nextStep = EnqueueingNextStep::NextStep::StoreInFailedJobsContainer;
    } else {
      // Status is unchanged
      ret.nextStatus = j.status();
      ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReport;
    }
    return ret;
  }
  throw NoSuchJob("In RetrieveRequest::addJobFailure(): could not find job");
}


//------------------------------------------------------------------------------
// RetrieveRequest::getLastActiveVid()
//------------------------------------------------------------------------------
std::string RetrieveRequest::getLastActiveVid() {
  checkPayloadReadable();
  auto activeCopyNb = m_payload.activecopynb();
  for (auto & tf: m_payload.archivefile().tapefiles()) {
    if (tf.copynb() == activeCopyNb)
      return tf.vid();
  }
  return m_payload.archivefile().tapefiles(0).vid();
}


//------------------------------------------------------------------------------
// RetrieveRequest::setSchedulerRequest()
//------------------------------------------------------------------------------
void RetrieveRequest::setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest& retrieveRequest) {
  checkPayloadWritable();
  auto *sr = m_payload.mutable_schedulerrequest();
  sr->mutable_requester()->set_name(retrieveRequest.requester.name);
  sr->mutable_requester()->set_group(retrieveRequest.requester.group);
  sr->set_archivefileid(retrieveRequest.archiveFileID);
  sr->set_dsturl(retrieveRequest.dstURL);
  sr->set_retrieveerrorreporturl(retrieveRequest.errorReportURL);
  DiskFileInfoSerDeser dfisd(retrieveRequest.diskFileInfo);
  dfisd.serialize(*sr->mutable_diskfileinfo());
  objectstore::EntryLogSerDeser el(retrieveRequest.creationLog);
  el.serialize(*sr->mutable_entrylog());
}

//------------------------------------------------------------------------------
// RetrieveRequest::getSchedulerRequest()
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveRequest RetrieveRequest::getSchedulerRequest() {
  checkPayloadReadable();
  common::dataStructures::RetrieveRequest ret;
  ret.requester.name = m_payload.schedulerrequest().requester().name();
  ret.requester.group = m_payload.schedulerrequest().requester().group();
  ret.archiveFileID = m_payload.schedulerrequest().archivefileid();
  objectstore::EntryLogSerDeser el(ret.creationLog);
  el.deserialize(m_payload.schedulerrequest().entrylog());
  ret.dstURL = m_payload.schedulerrequest().dsturl();
  ret.errorReportURL = m_payload.schedulerrequest().retrieveerrorreporturl();
  objectstore::DiskFileInfoSerDeser dfisd;
  dfisd.deserialize(m_payload.schedulerrequest().diskfileinfo());
  ret.diskFileInfo = dfisd;
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getArchiveFile()
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile RetrieveRequest::getArchiveFile() {
  objectstore::ArchiveFileSerDeser af;
  af.deserialize(m_payload.archivefile());
  return af;
}


//------------------------------------------------------------------------------
// RetrieveRequest::setRetrieveFileQueueCriteria()
//------------------------------------------------------------------------------
void RetrieveRequest::setRetrieveFileQueueCriteria(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  checkPayloadWritable();
  ArchiveFileSerDeser(criteria.archiveFile).serialize(*m_payload.mutable_archivefile());
  for (auto &tf: criteria.archiveFile.tapeFiles) {
    MountPolicySerDeser(criteria.mountPolicy).serialize(*m_payload.mutable_mountpolicy());
    const uint32_t hardcodedRetriesWithinMount = 3;
    const uint32_t hardcodedTotalRetries = 6;
    const uint32_t hardcodedReportRetries = 2;
    addJob(tf.second.copyNb, hardcodedRetriesWithinMount, hardcodedTotalRetries, hardcodedReportRetries);
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::dumpJobs()
//------------------------------------------------------------------------------
auto RetrieveRequest::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  for (auto & j: m_payload.jobs()) {
    ret.push_back(JobDump());
    ret.back().copyNb=j.copynb();
    ret.back().status=j.status();
    // TODO: status
  }
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getJob()
//------------------------------------------------------------------------------
auto  RetrieveRequest::getJob(uint16_t copyNb) -> JobDump {
  checkPayloadReadable();
  // find the job
  for (auto & j: m_payload.jobs()) {
    if (j.copynb()==copyNb) {
      JobDump ret;
      ret.copyNb=copyNb;
      ret.status=j.status();
      return ret;
    }
  }
  throw NoSuchJob("In objectstore::RetrieveRequest::getJob(): job not found for this copyNb");
}

//------------------------------------------------------------------------------
// RetrieveRequest::getJobs()
//------------------------------------------------------------------------------
auto RetrieveRequest::getJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  for (auto & j: m_payload.jobs()) {
    ret.push_back(JobDump());
    ret.back().copyNb=j.copynb();
    ret.back().status=j.status();
  }
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::addJobFailure()
//------------------------------------------------------------------------------
bool RetrieveRequest::addJobFailure(uint16_t copyNumber, uint64_t mountId, 
    const std::string & failureReason, log::LogContext & lc) {
  checkPayloadWritable();
  // Find the job and update the number of failures
  // (and return the full request status: failed (true) or to be retried (false))
  // The request will go through a full requeueing if retried (in caller).
  for (size_t i=0; i<(size_t)m_payload.jobs_size(); i++) {
    auto &j=*m_payload.mutable_jobs(i);
    if (j.copynb() == copyNumber) {
      if (j.lastmountwithfailure() == mountId) {
        j.set_retrieswithinmount(j.retrieswithinmount() + 1);
      } else {
        j.set_retrieswithinmount(1);
        j.set_lastmountwithfailure(mountId);
      }
      j.set_totalretries(j.totalretries() + 1);
      * j.mutable_failurelogs()->Add() = failureReason;
    }
    if (j.totalretries() >= j.maxtotalretries()) {
      j.set_status(serializers::RetrieveJobStatus::RJS_ToReportForFailure);
      for (auto & j2: m_payload.jobs()) 
        if (j2.status() == serializers::RetrieveJobStatus::RJS_ToTransfer) return false;
      return true;
    } else {
      j.set_status(serializers::RetrieveJobStatus::RJS_ToTransfer);
      return false;
    }
  }
  throw NoSuchJob ("In RetrieveRequest::addJobFailure(): could not find job");
}

//------------------------------------------------------------------------------
// RetrieveRequest::getRetryStatus()
//------------------------------------------------------------------------------
RetrieveRequest::RetryStatus RetrieveRequest::getRetryStatus(const uint16_t copyNumber) {
  checkPayloadReadable();
  for (auto &j: m_payload.jobs()) {
    if (copyNumber == j.copynb()) {
      RetryStatus ret;
      ret.retriesWithinMount = j.retrieswithinmount();
      ret.maxRetriesWithinMount = j.maxretrieswithinmount();
      ret.totalRetries = j.totalretries();
      ret.maxTotalRetries = j.maxtotalretries();
      ret.totalReportRetries = j.totalreportretries();
      ret.maxReportRetries = j.maxreportretries();
      return ret;
    }
  }
  throw cta::exception::Exception("In RetrieveRequest::getRetryStatus(): job not found()");
}

//------------------------------------------------------------------------------
// RetrieveRequest::getQueueType()
//------------------------------------------------------------------------------
JobQueueType RetrieveRequest::getQueueType() {
  checkPayloadReadable();
  bool hasToReport=false;
  for (auto &j: m_payload.jobs()) {
    // Any job is to be transfered => To transfer
    switch(j.status()) {
    case serializers::RetrieveJobStatus::RJS_ToTransfer:
      return JobQueueType::JobsToTransfer;
      break;
    case serializers::RetrieveJobStatus::RJS_ToReportForFailure:
      // Else any job to report => to report.
      hasToReport=true;
      break;
    default: break;
    }
  }
  if (hasToReport) return JobQueueType::JobsToReport;
  return JobQueueType::FailedJobs;
}

//------------------------------------------------------------------------------
// RetrieveRequest::statusToString()
//------------------------------------------------------------------------------
std::string RetrieveRequest::statusToString(const serializers::RetrieveJobStatus& status) {
  switch(status) {
  case serializers::RetrieveJobStatus::RJS_ToTransfer:
    return "ToTransfer";
  case serializers::RetrieveJobStatus::RJS_Failed:
    return "Failed";
  default:
    return std::string("Unknown (")+std::to_string((uint64_t) status)+")";
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::eventToString()
//------------------------------------------------------------------------------
std::string RetrieveRequest::eventToString(JobEvent jobEvent) {
  switch(jobEvent) {
    case JobEvent::ReportFailed:   return "ReportFailed";
    case JobEvent::TransferFailed: return "EventFailed";
  }
  return std::string("Unknown (") + std::to_string(static_cast<unsigned int>(jobEvent)) + ")";
}


//------------------------------------------------------------------------------
// RetrieveRequest::determineNextStep()
//------------------------------------------------------------------------------
auto RetrieveRequest::determineNextStep(uint16_t copyNumberUpdated, JobEvent jobEvent, 
    log::LogContext& lc) -> EnqueueingNextStep
{
  checkPayloadWritable();
  auto &jl = m_payload.jobs();
  using serializers::RetrieveJobStatus;

  // Validate the current status
  //
  // Get status
  cta::optional<RetrieveJobStatus> currentStatus;
  for (auto &j : jl) {
    if(j.copynb() == copyNumberUpdated) currentStatus = j.status();
  }
  if (!currentStatus) {
    std::stringstream err;
    err << "In RetrieveRequest::updateJobStatus(): copynb not found : " << copyNumberUpdated << ", exiting ones: ";
    for(auto &j : jl) err << j.copynb() << "  ";
    throw cta::exception::Exception(err.str());
  }
  // Check status compatibility with event
  switch (jobEvent)
  {
    case JobEvent::TransferFailed:
      if (*currentStatus != RetrieveJobStatus::RJS_ToTransfer) {
        // Wrong status, but the context leaves no ambiguity. Just warn.
        log::ScopedParamContainer params(lc);
        params.add("event", eventToString(jobEvent))
              .add("status", statusToString(*currentStatus))
              .add("fileId", m_payload.archivefile().archivefileid());
        lc.log(log::WARNING, "In RetrieveRequest::updateJobStatus(): unexpected status. Assuming ToTransfer.");
      }
      break;
    case JobEvent::ReportFailed:
      if(*currentStatus != RetrieveJobStatus::RJS_ToReportForFailure) {
        // Wrong status, but end status will be the same anyway
        log::ScopedParamContainer params(lc);
        params.add("event", eventToString(jobEvent))
              .add("status", statusToString(*currentStatus))
              .add("fileId", m_payload.archivefile().archivefileid());
        lc.log(log::WARNING, "In RetrieveRequest::updateJobStatus(): unexpected status. Failing the job.");
      }
  }
  // We are in the normal cases now
  EnqueueingNextStep ret;
  switch(jobEvent)
  {  
    case JobEvent::TransferFailed:
      ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReport;
      ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToReportForFailure;
      break;

    case JobEvent::ReportFailed:
      ret.nextStep = EnqueueingNextStep::NextStep::StoreInFailedJobsContainer;
      ret.nextStatus = serializers::RetrieveJobStatus::RJS_Failed;
  }
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getJobStatus()
//------------------------------------------------------------------------------
serializers::RetrieveJobStatus RetrieveRequest::getJobStatus(uint16_t copyNumber) {
  checkPayloadReadable();
  for (auto & j: m_payload.jobs())
    if (j.copynb() == copyNumber)
      return j.status();
  std::stringstream err;
  err << "In RetrieveRequest::getJobStatus(): could not find job for copynb=" << copyNumber;
  throw exception::Exception(err.str());
}

//------------------------------------------------------------------------------
// RetrieveRequest::asyncUpdateJobOwner()
//------------------------------------------------------------------------------
auto RetrieveRequest::asyncUpdateJobOwner(uint16_t copyNumber, const std::string &owner,
  const std::string &previousOwner) -> AsyncJobOwnerUpdater*
{
  std::unique_ptr<AsyncJobOwnerUpdater> ret(new AsyncJobOwnerUpdater);
  // The unique pointer will be std::moved so we need to work with its content (bare pointer or here ref to content).
  auto & retRef = *ret;
  ret->m_updaterCallback=
      [this, copyNumber, owner, previousOwner, &retRef](const std::string &in)->std::string {
        // We have a locked and fetched object, so we just need to work on its representation.
        serializers::ObjectHeader oh;
        if (!oh.ParseFromString(in)) {
          // Use a the tolerant parser to assess the situation.
          oh.ParsePartialFromString(in);
          throw cta::exception::Exception(std::string("In RetrieveRequest::asyncUpdateJobOwner(): could not parse header: ")+
            oh.InitializationErrorString());
        }
        if (oh.type() != serializers::ObjectType::RetrieveRequest_t) {
          std::stringstream err;
          err << "In RetrieveRequest::asyncUpdateJobOwner()::lambda(): wrong object type: " << oh.type();
          throw cta::exception::Exception(err.str());
        }
        // We don't need to deserialize the payload to update owner...
        if (oh.owner() != previousOwner)
          throw Backend::WrongPreviousOwner("In RetrieveRequest::asyncUpdateJobOwner()::lambda(): Request not owned.");
        oh.set_owner(owner);
        // ... but we still need to extract information
        serializers::RetrieveRequest payload;
        if (!payload.ParseFromString(oh.payload())) {
          // Use a the tolerant parser to assess the situation.
          payload.ParsePartialFromString(oh.payload());
          throw cta::exception::Exception(std::string("In RetrieveRequest::asyncUpdateJobOwner(): could not parse payload: ")+
            payload.InitializationErrorString());
        }
        // Find the copy number
        auto jl=payload.jobs();
        for (auto & j: jl) {
          if (j.copynb() == copyNumber) {
            // We also need to gather all the job content for the user to get in-memory
            // representation.
            // TODO this is an unfortunate duplication of the getXXX() members of ArchiveRequest.
            // We could try and refactor this.
            retRef.m_retrieveRequest.archiveFileID = payload.archivefile().archivefileid();
            objectstore::EntryLogSerDeser el;
            el.deserialize(payload.schedulerrequest().entrylog());
            retRef.m_retrieveRequest.creationLog = el;
            objectstore::DiskFileInfoSerDeser dfi;
            dfi.deserialize(payload.schedulerrequest().diskfileinfo());
            retRef.m_retrieveRequest.diskFileInfo = dfi;
            retRef.m_retrieveRequest.dstURL = payload.schedulerrequest().dsturl();
            retRef.m_retrieveRequest.errorReportURL = payload.schedulerrequest().retrieveerrorreporturl();
            retRef.m_retrieveRequest.requester.name = payload.schedulerrequest().requester().name();
            retRef.m_retrieveRequest.requester.group = payload.schedulerrequest().requester().group();
            objectstore::ArchiveFileSerDeser af;
            af.deserialize(payload.archivefile());
            retRef.m_archiveFile = af;
            oh.set_payload(payload.SerializePartialAsString());
            return oh.SerializeAsString();
          }
        }
        // If we do not find the copy, return not owned as well...
        throw Backend::WrongPreviousOwner("In RetrieveRequest::asyncUpdateJobOwner()::lambda(): copyNb not found.");
      };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(), ret->m_updaterCallback));
  return ret.release();
  }

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::wait()
//------------------------------------------------------------------------------
void RetrieveRequest::AsyncJobOwnerUpdater::wait() {
  m_backendUpdater->wait();
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::getArchiveFile()
//------------------------------------------------------------------------------
const common::dataStructures::ArchiveFile& RetrieveRequest::AsyncJobOwnerUpdater::getArchiveFile() {
  return m_archiveFile;
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::getRetrieveRequest()
//------------------------------------------------------------------------------
const common::dataStructures::RetrieveRequest& RetrieveRequest::AsyncJobOwnerUpdater::getRetrieveRequest() {
  return m_retrieveRequest;
}

//------------------------------------------------------------------------------
// RetrieveRequest::setActiveCopyNumber()
//------------------------------------------------------------------------------
void RetrieveRequest::setActiveCopyNumber(uint32_t activeCopyNb) {
  checkPayloadWritable();
  m_payload.set_activecopynb(activeCopyNb);
}

//------------------------------------------------------------------------------
// RetrieveRequest::getActiveCopyNumber()
//------------------------------------------------------------------------------
uint32_t RetrieveRequest::getActiveCopyNumber() {
  return m_payload.activecopynb();
}

//------------------------------------------------------------------------------
// RetrieveRequest::getRetrieveFileQueueCriteria()
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveFileQueueCriteria RetrieveRequest::getRetrieveFileQueueCriteria() {
  checkPayloadReadable();
  cta::common::dataStructures::RetrieveFileQueueCriteria ret;
  ArchiveFileSerDeser afsd;
  afsd.deserialize(m_payload.archivefile());
  ret.archiveFile = afsd;
  MountPolicySerDeser mpsd;
  mpsd.deserialize(m_payload.mountpolicy());
  ret.mountPolicy  = mpsd;
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getEntryLog()
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog RetrieveRequest::getEntryLog() {
  checkPayloadReadable();
  EntryLogSerDeser el;
  el.deserialize(m_payload.schedulerrequest().entrylog());
  return el;
}

//------------------------------------------------------------------------------
// RetrieveRequest::dump()
//------------------------------------------------------------------------------
std::string RetrieveRequest::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

//------------------------------------------------------------------------------
// RetrieveRequest::asyncDeleteJob()
//------------------------------------------------------------------------------
RetrieveRequest::AsyncJobDeleter * RetrieveRequest::asyncDeleteJob() {
  std::unique_ptr<AsyncJobDeleter> ret(new AsyncJobDeleter);
  ret->m_backendDeleter.reset(m_objectStore.asyncDelete(getAddressIfSet()));
  return ret.release();
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobDeleter::wait()
//------------------------------------------------------------------------------
void RetrieveRequest::AsyncJobDeleter::wait() {
  m_backendDeleter->wait();
}

//------------------------------------------------------------------------------
// RetrieveRequest::getFailures()
//------------------------------------------------------------------------------
std::list<std::string> RetrieveRequest::getFailures() {
  checkPayloadReadable();
  std::list<std::string> ret;
  for (auto &j: m_payload.jobs()) {
    for (auto &f: j.failurelogs()) {
      ret.push_back(f);
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::setFailureReason()
//------------------------------------------------------------------------------
void RetrieveRequest::setFailureReason(const std::string& reason) {
  checkPayloadWritable();
  m_payload.set_failurereportlog(reason);
}

//------------------------------------------------------------------------------
// RetrieveRequest::setJobStatus()
//------------------------------------------------------------------------------
void RetrieveRequest::setJobStatus(uint64_t copyNumber, const serializers::RetrieveJobStatus& status) {
  checkPayloadWritable();
  for (auto j = m_payload.mutable_jobs()->begin(); j != m_payload.mutable_jobs()->end(); j++) {
    if (j->copynb() == copyNumber) {
      j->set_status(status);
      return;
    }
  }
  throw exception::Exception("In RetrieveRequest::setJobStatus(): job not found.");
}

}} // namespace cta::objectstore
