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
  // The owner is indeed the right one. We should requeue the request if possible.
  // Find the vids for active jobs in the request (pending ones).
  using serializers::RetrieveJobStatus;
  std::set<RetrieveJobStatus> validStates({RetrieveJobStatus::RJS_Pending, RetrieveJobStatus::RJS_Selected});
  std::set<std::string> candidateVids;
  for (auto &j: m_payload.jobs()) {
    if (validStates.count(j.status())) {
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
  // If there is no candidate, we cancel the job
  // TODO: in the future, we might queue it for reporting to EOS.
  if (candidateVids.empty()) {
    remove();
    log::ScopedParamContainer params(lc);
    params.add("jobObject", getAddressIfSet());
    lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): deleted job as no tape file is available for recall.");
    return;
  }
  // If we have to fetch the status of the tapes and queued for the non-disabled vids.
  auto bestVid=Helpers::selectBestRetrieveQueue(candidateVids, catalogue, m_objectStore);
  // Find the corresponding tape file, which will give the copynb, which will allow finding the retrieve job.
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
  // We now need to grab the queue a requeue the request.
  RetrieveQueue rq(m_objectStore);
  ScopedExclusiveLock rql;
  Helpers::getLockedAndFetchedQueue<RetrieveQueue>(rq, rql, agentReference, bestVid, QueueType::LiveJobs, lc);
  // Enqueue add the job to the queue
  objectstore::MountPolicySerDeser mp;
  mp.deserialize(m_payload.mountpolicy());
  std::list<RetrieveQueue::JobToAdd> jta;
  jta.push_back({bestTapeFile->copynb(), bestTapeFile->fseq(), getAddressIfSet(), m_payload.archivefile().filesize(), 
    mp, (signed)m_payload.schedulerrequest().entrylog().time()});
  rq.addJobsIfNecessaryAndCommit(jta, agentReference, lc);
  auto jobsSummary=rq.getJobsSummary();
  auto queueUpdateTime = t.secs(utils::Timer::resetCounter);
  // We can now make the transition official
  bestJob->set_status(serializers::RetrieveJobStatus::RJS_Pending);
  m_payload.set_activecopynb(bestJob->copynb());
  setOwner(rq.getAddressIfSet());
  commit();
  Helpers::updateRetrieveQueueStatisticsCache(bestVid, jobsSummary.files, jobsSummary.bytes, jobsSummary.priority);
  rql.release();
  auto commitUnlockQueueTime = t.secs(utils::Timer::resetCounter);
  {
    log::ScopedParamContainer params(lc);
    params.add("jobObject", getAddressIfSet())
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

//------------------------------------------------------------------------------
// RetrieveRequest::addJob()
//------------------------------------------------------------------------------
void RetrieveRequest::addJob(uint64_t copyNb, uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries) {
  checkPayloadWritable();
  auto *tf = m_payload.add_jobs();
  tf->set_copynb(copyNb);
  tf->set_lastmountwithfailure(0);
  tf->set_maxretrieswithinmount(maxRetiesWithinMount);
  tf->set_maxtotalretries(maxTotalRetries);
  tf->set_retrieswithinmount(0);
  tf->set_totalretries(0);
  tf->set_status(serializers::RetrieveJobStatus::RJS_Pending);
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
    addJob(tf.second.copyNb, hardcodedRetriesWithinMount, hardcodedTotalRetries);
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
      j.set_status(serializers::RJS_Failed);
      bool ret=finishIfNecessary(lc);
      if (!ret) commit();
      return ret;
    } else {
      j.set_status(serializers::RJS_Pending);
      commit();
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
      return ret;
    }
  }
  throw cta::exception::Exception("In RetrieveRequest::getRetryStatus(): job not found()");
}


//------------------------------------------------------------------------------
// RetrieveRequest::statusToString()
//------------------------------------------------------------------------------
std::string RetrieveRequest::statusToString(const serializers::RetrieveJobStatus& status) {
  switch(status) {
  case serializers::RetrieveJobStatus::RJS_Complete:
    return "Complete";
  case serializers::RetrieveJobStatus::RJS_Failed:
    return "Failed";
  case serializers::RetrieveJobStatus::RJS_LinkingToTape:
    return "LinkingToTape";
  case serializers::RetrieveJobStatus::RJS_Pending:
    return "Pending";
  case serializers::RetrieveJobStatus::RJS_Selected:
    return "Selected";
  default:
    return std::string("Unknown (")+std::to_string((uint64_t) status)+")";
  }
}


//------------------------------------------------------------------------------
// RetrieveRequest::finishIfNecessary()
//------------------------------------------------------------------------------
bool RetrieveRequest::finishIfNecessary(log::LogContext & lc) {
  checkPayloadWritable();
  // This function is typically called after changing the status of one job
  // in memory. If the request is complete, we will just remove it.
  // If all the jobs are either complete or failed, we can remove the request.
  auto & jl=m_payload.jobs();
  using serializers::RetrieveJobStatus;
  std::set<serializers::RetrieveJobStatus> finishedStatuses(
    {RetrieveJobStatus::RJS_Complete, RetrieveJobStatus::RJS_Failed});
  for (auto & j: jl)
    if (!finishedStatuses.count(j.status()))
      return false;
  remove();
  log::ScopedParamContainer params(lc);
  params.add("retrieveRequestObject", getAddressIfSet());
  for (auto & j: jl) {
    params.add(std::string("statusForCopyNb")+std::to_string(j.copynb()), statusToString(j.status()));
  }
  lc.log(log::INFO, "In RetrieveRequest::finishIfNecessary(): removed finished retrieve request.");
  return true;
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
// RetrieveRequest::asyncUpdateOwner()
//------------------------------------------------------------------------------
auto RetrieveRequest::asyncUpdateOwner(uint16_t copyNumber, const std::string& owner, const std::string& previousOwner) 
  -> AsyncOwnerUpdater* {
  std::unique_ptr<AsyncOwnerUpdater> ret(new AsyncOwnerUpdater);
  // Passing a reference to the unique pointer led to strange behaviors.
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
          throw WrongPreviousOwner("In RetrieveRequest::asyncUpdateJobOwner()::lambda(): Request not owned.");
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
            retRef.m_retieveRequest.archiveFileID = payload.archivefile().archivefileid();
            objectstore::EntryLogSerDeser el;
            el.deserialize(payload.schedulerrequest().entrylog());
            retRef.m_retieveRequest.creationLog = el;
            objectstore::DiskFileInfoSerDeser dfi;
            dfi.deserialize(payload.schedulerrequest().diskfileinfo());
            retRef.m_retieveRequest.diskFileInfo = dfi;
            retRef.m_retieveRequest.dstURL = payload.schedulerrequest().dsturl();
            retRef.m_retieveRequest.errorReportURL = payload.schedulerrequest().retrieveerrorreporturl();
            retRef.m_retieveRequest.requester.name = payload.schedulerrequest().requester().name();
            retRef.m_retieveRequest.requester.group = payload.schedulerrequest().requester().group();
            objectstore::ArchiveFileSerDeser af;
            af.deserialize(payload.archivefile());
            retRef.m_archiveFile = af;
            oh.set_payload(payload.SerializePartialAsString());
            return oh.SerializeAsString();
          }
        }
        // If we do not find the copy, return not owned as well...
        throw WrongPreviousOwner("In RetrieveRequest::asyncUpdateJobOwner()::lambda(): copyNb not found.");
      };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(), ret->m_updaterCallback));
  return ret.release();
  }

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncOwnerUpdater::wait()
//------------------------------------------------------------------------------
void RetrieveRequest::AsyncOwnerUpdater::wait() {
  m_backendUpdater->wait();
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncOwnerUpdater::getArchiveFile()
//------------------------------------------------------------------------------
const common::dataStructures::ArchiveFile& RetrieveRequest::AsyncOwnerUpdater::getArchiveFile() {
  return m_archiveFile;
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncOwnerUpdater::getRetrieveRequest()
//------------------------------------------------------------------------------
const common::dataStructures::RetrieveRequest& RetrieveRequest::AsyncOwnerUpdater::getRetrieveRequest() {
  return m_retieveRequest;
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
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
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


}} // namespace cta::objectstore

