/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
#include "LifecycleTimingsSerDeser.hpp"
#include "Sorter.hpp"
#include "AgentWrapper.hpp"
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
  m_payload.set_isrepack(false);
  m_payload.set_isverifyonly(false);
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
    switch(j.status()){
      case RetrieveJobStatus::RJS_ToTransfer:
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
        break;
      case RetrieveJobStatus::RJS_ToReportToRepackForSuccess:
      case RetrieveJobStatus::RJS_ToReportToRepackForFailure:
        //We don't have any vid to find, we just need to
        //Requeue it into RetrieveQueueToReportToRepackForSuccess or into the RetrieveQueueToReportToRepackForFailure (managed by the sorter)
        for (auto &tf: m_payload.archivefile().tapefiles()) {
          if (tf.copynb() == j.copynb()) {
            Sorter sorter(agentReference,m_objectStore,catalogue);
            std::shared_ptr<RetrieveRequest> rr = std::make_shared<RetrieveRequest>(*this);
            cta::objectstore::Agent agentRR(getOwner(),m_objectStore);
            cta::objectstore::AgentWrapper agentRRWrapper(agentRR);
            sorter.insertRetrieveRequest(rr,agentRRWrapper,cta::optional<uint32_t>(tf.copynb()),lc);
            std::string retrieveQueueAddress = rr->getRepackInfo().repackRequestAddress;
            this->m_exclusiveLock->release();
            cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
            std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFutures;
            cta::utils::Timer t;
            cta::log::TimingList tl;
            for(auto& kv: allRetrieveJobs){
              for(auto& job: kv.second){
                allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
              }
            }
            sorter.flushAll(lc);
            tl.insertAndReset("sorterFlushingTime",t);
            for(auto& future: allFutures){
              //Throw an exception in case of failure
              std::get<1>(future).get();
            }
            log::ScopedParamContainer params(lc);
            params.add("jobObject", getAddressIfSet())
                  .add("fileId", m_payload.archivefile().archivefileid())
                  .add("queueObject", retrieveQueueAddress)
                  .add("copynb", tf.copynb())
                  .add("tapeVid", tf.vid());
            tl.addToLog(params);
            lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): requeued the repack retrieve request.");
            return;
          }
        }
        break;
      default:
        break;
    }
    found:;
  }
  std::string bestVid;
  // If no tape file is a candidate, we just need to skip to queueing to the failed queue
  if (candidateVids.empty()) goto queueForFailure;
  // We have a chance to find an available tape. Let's compute best VID (this will
  // filter on tape availability.
  try {
    // If we have to fetch the status of the tapes and queued for the non-disabled vids.
    bestVid=Helpers::selectBestRetrieveQueue(candidateVids, catalogue, m_objectStore,m_payload.repack_info().force_disabled_tape());
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
            params.add("tapeVid", tf.vid())
                  .add("fSeq", tf.fseq());
            break;
          }
        }
        // Generate the last failure for this job (tape unavailable).
        *j.mutable_failurelogs()->Add() = utils::getCurrentLocalTime() + " " +
            utils::getShortHostname() + " In RetrieveRequest::garbageCollect(): No VID available to requeue the request. Failing it.";
        lc.log(log::ERR, "In RetrieveRequest::garbageCollect(): No VID available to requeue the request. Failing all jobs.");
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
    Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq, rql, agentReference, activeVid, getQueueType(), lc);
    // Enqueue the job
    objectstore::MountPolicySerDeser mp;
    std::list<RetrieveQueue::JobToAdd> jta;
    jta.push_back({activeCopyNb, activeFseq, getAddressIfSet(), m_payload.archivefile().filesize(),
      mp, (signed)m_payload.schedulerrequest().entrylog().time(), nullopt, nullopt});
    if (m_payload.has_activity()) {
      jta.back().activity = m_payload.activity();
    }
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
            .add("tapeVid", activeVid)
            .add("queueUpdateTime", queueUpdateTime)
            .add("commitUnlockQueueTime", commitUnlockQueueTime);
      lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): queued the request to the failed queue.");
    }
    return;
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
      Helpers::getLockedAndFetchedJobQueue<RetrieveQueue>(rq, rql, agentReference, bestVid,
        common::dataStructures::JobQueueType::JobsToTransferForUser, lc);
      // Enqueue the job
    objectstore::MountPolicySerDeser mp;
    mp.deserialize(m_payload.mountpolicy());
    std::list<RetrieveQueue::JobToAdd> jta;
    jta.push_back({bestTapeFile->copynb(), bestTapeFile->fseq(), getAddressIfSet(), m_payload.archivefile().filesize(),
      mp, (signed)m_payload.schedulerrequest().entrylog().time(), getActivity(), getDiskSystemName()});
    if (m_payload.has_activity()) {
      jta.back().activity = m_payload.activity();
    }
    rq.addJobsIfNecessaryAndCommit(jta, agentReference, lc);
    auto jobsSummary=rq.getJobsSummary();
    auto queueUpdateTime = t.secs(utils::Timer::resetCounter);
    // We can now make the transition official.
    m_payload.set_activecopynb(bestJob->copynb());
    setOwner(rq.getAddressIfSet());
    commit();
    Helpers::updateRetrieveQueueStatisticsCache(bestVid, jobsSummary.jobs, jobsSummary.bytes, jobsSummary.priority);
    rql.release();
    auto commitUnlockQueueTime = t.secs(utils::Timer::resetCounter);
    {
      log::ScopedParamContainer params(lc);
      params.add("jobObject", getAddressIfSet())
            .add("fileId", m_payload.archivefile().archivefileid())
            .add("queueObject", rq.getAddressIfSet())
            .add("copynb", bestTapeFile->copynb())
            .add("tapeVid", bestTapeFile->vid())
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
            .add("tapeVid", bestTapeFile->vid())
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
void RetrieveRequest::addJob(uint32_t copyNb, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries,
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
auto RetrieveRequest::addTransferFailure(uint32_t copyNumber, uint64_t mountId, const std::string &failureReason,
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
    if(m_payload.isverifyonly()) {
      // Don't retry verification jobs, they should fail immediately
      return determineNextStep(copyNumber, JobEvent::TransferFailed, lc);
    }
    if(j.totalretries() < j.maxtotalretries()) {
      EnqueueingNextStep ret;
      ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToTransfer;
      if(j.retrieswithinmount() < j.maxretrieswithinmount())
        // Job can try again within this mount
        ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForTransferForUser;
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
auto RetrieveRequest::addReportFailure(uint32_t copyNumber, uint64_t sessionId, const std::string &failureReason,
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
      ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReportForUser;
    }
    return ret;
  }
  throw NoSuchJob("In RetrieveRequest::addJobFailure(): could not find job");
}

auto RetrieveRequest::addReportAbort(uint32_t copyNumber, uint64_t mountId, const std::string &abortReason,
  log::LogContext &lc) -> EnqueueingNextStep
{
  checkPayloadWritable();
  EnqueueingNextStep ret;
  for(int i = 0; i < m_payload.jobs_size(); ++i)
  {
    auto &j = *m_payload.mutable_jobs(i);
    if(j.copynb() == copyNumber) {
      if(j.lastmountwithfailure() == mountId) {
        j.set_retrieswithinmount(j.retrieswithinmount() + 1);
      } else {
        j.set_retrieswithinmount(1);
        j.set_lastmountwithfailure(mountId);
      }
      j.set_totalretries(j.totalretries() + 1);
      *j.mutable_failurelogs()->Add() = abortReason;
    }
  }

  if(this->getRepackInfo().isRepack){
    ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure;
    ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReportForRepack;
  } else {
    ret.nextStatus = serializers::RetrieveJobStatus::RJS_Failed;
    ret.nextStep = EnqueueingNextStep::NextStep::StoreInFailedJobsContainer;
  }
  return ret;
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
  sr->set_isverifyonly(retrieveRequest.isVerifyOnly);
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
  objectstore::EntryLogSerDeser el;
  el.deserialize(m_payload.schedulerrequest().entrylog());
  ret.creationLog = el;
  ret.dstURL = m_payload.schedulerrequest().dsturl();
  ret.errorReportURL = m_payload.schedulerrequest().retrieveerrorreporturl();
  ret.isVerifyOnly = m_payload.schedulerrequest().isverifyonly();
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
    m_payload.set_mountpolicyname(criteria.mountPolicy.name);
    /*
     * Explaination about these hardcoded retries :
     * The hardcoded RetriesWithinMount will ensure that we will try to retrieve the file 3 times
     * in the same mount.
     * The hardcoded TotalRetries ensure that we will never try more than 6 times to retrieve a file.
     * As totalretries = 6 and retrieswithinmount = 3, this will ensure that the file will be retried by maximum 2 mounts.
     * (2 mounts * 3 retrieswithinmount = 6 totalretries)
     */
    const uint32_t hardcodedRetriesWithinMount = 3;
    const uint32_t hardcodedTotalRetries = 6;
    const uint32_t hardcodedReportRetries = 2;
    addJob(tf.copyNb, hardcodedRetriesWithinMount, hardcodedTotalRetries, hardcodedReportRetries);
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::setActivityIfNeeded()
//------------------------------------------------------------------------------
void RetrieveRequest::setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest& retrieveRequest,
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  checkPayloadWritable();
  if (retrieveRequest.activity) {
    m_payload.set_activity(retrieveRequest.activity.value());
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::getActivity()
//------------------------------------------------------------------------------
optional<std::string> RetrieveRequest::getActivity() {
  checkPayloadReadable();
  optional<std::string> ret;
  if (m_payload.has_activity()) {
    ret = m_payload.activity();
  }
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::setDiskSystemName()
//------------------------------------------------------------------------------
void RetrieveRequest::setDiskSystemName(const std::string& diskSystemName) {
  checkPayloadWritable();
  m_payload.set_disk_system_name(diskSystemName);
}

//------------------------------------------------------------------------------
// RetrieveRequest::getDiskSystemName()
//------------------------------------------------------------------------------
optional<std::string> RetrieveRequest::getDiskSystemName() {
  checkPayloadReadable();
  optional<std::string> ret;
  if (m_payload.has_disk_system_name())
    ret = m_payload.disk_system_name();
  return ret;
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
auto  RetrieveRequest::getJob(uint32_t copyNb) -> JobDump {
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
bool RetrieveRequest::addJobFailure(uint32_t copyNumber, uint64_t mountId,
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
      j.set_status(serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure);
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
// RetrieveRequest::setRepackInfo()
//------------------------------------------------------------------------------
void RetrieveRequest::setRepackInfo(const RepackInfo& repackInfo) {
  checkPayloadWritable();
  m_payload.set_isrepack(repackInfo.isRepack);
  if (repackInfo.isRepack) {
    for (auto & ar: repackInfo.archiveRouteMap) {
      auto * plar=m_payload.mutable_repack_info()->mutable_archive_routes()->Add();
      plar->set_copynb(ar.first);
      plar->set_tapepool(ar.second);
    }
    for (auto cntr: repackInfo.copyNbsToRearchive) {
      m_payload.mutable_repack_info()->mutable_copy_nbs_to_rearchive()->Add(cntr);
    }

    m_payload.mutable_repack_info()->set_has_user_provided_file(repackInfo.hasUserProvidedFile);
    m_payload.mutable_repack_info()->set_force_disabled_tape(repackInfo.forceDisabledTape);
    m_payload.mutable_repack_info()->set_file_buffer_url(repackInfo.fileBufferURL);
    m_payload.mutable_repack_info()->set_repack_request_address(repackInfo.repackRequestAddress);
    m_payload.mutable_repack_info()->set_fseq(repackInfo.fSeq);
  }
}

//------------------------------------------------------------------------------
// RetrieveRequest::getRepackInfo()
//------------------------------------------------------------------------------
RetrieveRequest::RepackInfo RetrieveRequest::getRepackInfo() {
  checkPayloadReadable();
  RepackInfoSerDeser ret;
  if (m_payload.isrepack())
    ret.deserialize(m_payload.repack_info());
  return ret;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getRetryStatus()
//------------------------------------------------------------------------------
RetrieveRequest::RetryStatus RetrieveRequest::getRetryStatus(const uint32_t copyNumber) {
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
common::dataStructures::JobQueueType RetrieveRequest::getQueueType() {
  checkPayloadReadable();
  bool hasToReport=false;
  for (auto &j: m_payload.jobs()) {
    // Any job is to be transfered => To transfer
    switch(j.status()) {
    case serializers::RetrieveJobStatus::RJS_ToTransfer:
      return common::dataStructures::JobQueueType::JobsToTransferForUser;
      break;
    case serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess:
      return common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess;
      break;
    case serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure:
      // Else any job to report => to report.
      hasToReport=true;
      break;
    case serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure:
      return common::dataStructures::JobQueueType::JobsToReportToRepackForFailure;
    default: break;
    }
  }
  if (hasToReport) return common::dataStructures::JobQueueType::JobsToReportToUser;
  return common::dataStructures::JobQueueType::FailedJobs;
}

//------------------------------------------------------------------------------
// RetrieveRequest::getQueueType()
//------------------------------------------------------------------------------
common::dataStructures::JobQueueType RetrieveRequest::getQueueType(uint32_t copyNb){
  checkPayloadReadable();
  for(auto &j: m_payload.jobs()){
    if(j.copynb() == copyNb){
      switch(j.status()){
        case serializers::RetrieveJobStatus::RJS_ToTransfer:
          return common::dataStructures::JobQueueType::JobsToTransferForUser;
        case serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess:
          return common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess;
        case serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure:
          return common::dataStructures::JobQueueType::JobsToReportToUser;
        case serializers::RetrieveJobStatus::RJS_Failed:
          return common::dataStructures::JobQueueType::FailedJobs;
        case serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure:
          return common::dataStructures::JobQueueType::JobsToReportToRepackForFailure;
        default:
          return common::dataStructures::JobQueueType::FailedJobs;
      }
    }
  }
  throw cta::exception::Exception("In RetrieveRequest::getJobQueueType(): Copy number not found.");
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
auto RetrieveRequest::determineNextStep(uint32_t copyNumberUpdated, JobEvent jobEvent,
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
      if(*currentStatus != RetrieveJobStatus::RJS_ToReportToUserForFailure) {
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
      if(m_payload.isrepack()){
        ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReportForRepack;
        ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure;
      } else {
        ret.nextStep = EnqueueingNextStep::NextStep::EnqueueForReportForUser;
        ret.nextStatus = serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure;
      }
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
serializers::RetrieveJobStatus RetrieveRequest::getJobStatus(uint32_t copyNumber) {
  checkPayloadReadable();
  for (auto & j: m_payload.jobs())
    if (j.copynb() == copyNumber)
      return j.status();
  std::stringstream err;
  err << "In RetrieveRequest::getJobStatus(): could not find job for copynb=" << copyNumber;
  throw exception::Exception(err.str());
}

void RetrieveRequest::updateLifecycleTiming(serializers::RetrieveRequest& payload, const cta::objectstore::serializers::RetrieveJob& retrieveJob){
  typedef ::cta::objectstore::serializers::RetrieveJobStatus RetrieveJobStatus;
  LifecycleTimingsSerDeser lifeCycleSerDeser;
  lifeCycleSerDeser.deserialize(payload.lifecycle_timings());
  switch(retrieveJob.status()){
    case RetrieveJobStatus::RJS_ToTransfer:
      if(retrieveJob.totalretries() == 0){
        //totalretries = 0 then this is the first selection of the request
        lifeCycleSerDeser.first_selected_time = time(nullptr);
      }
      break;
    default:
      break;
  }
  lifeCycleSerDeser.serialize(*payload.mutable_lifecycle_timings());
}
//------------------------------------------------------------------------------
// RetrieveRequest::asyncUpdateJobOwner()
//------------------------------------------------------------------------------
auto RetrieveRequest::asyncUpdateJobOwner(uint32_t copyNumber, const std::string &owner,
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
        auto jl = payload.jobs();
        for (auto &j: jl) {
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
            retRef.m_retrieveRequest.isVerifyOnly = payload.schedulerrequest().isverifyonly();
            retRef.m_retrieveRequest.requester.name = payload.schedulerrequest().requester().name();
            retRef.m_retrieveRequest.requester.group = payload.schedulerrequest().requester().group();
            objectstore::ArchiveFileSerDeser af;
            af.deserialize(payload.archivefile());
            retRef.m_archiveFile = af;
            retRef.m_jobStatus = j.status();
            if (payload.has_activity()) {
              retRef.m_activity = payload.activity();
            }
            if (payload.has_disk_system_name())
              retRef.m_diskSystemName = payload.disk_system_name();
            RetrieveRequest::updateLifecycleTiming(payload,j);
            LifecycleTimingsSerDeser lifeCycleSerDeser;
            lifeCycleSerDeser.deserialize(payload.lifecycle_timings());
            retRef.m_retrieveRequest.lifecycleTimings = lifeCycleSerDeser;
            if (payload.isrepack()) {
              RetrieveRequest::RepackInfo & ri = retRef.m_repackInfo;
              for (auto &ar: payload.repack_info().archive_routes()) {
                ri.archiveRouteMap[ar.copynb()] = ar.tapepool();
              }
              for (auto cntr: payload.repack_info().copy_nbs_to_rearchive()) {
                ri.copyNbsToRearchive.insert(cntr);
              }
              if(payload.repack_info().has_has_user_provided_file()){
                ri.hasUserProvidedFile = payload.repack_info().has_user_provided_file();
              }
              ri.fileBufferURL = payload.repack_info().file_buffer_url();
              ri.isRepack = true;
              ri.repackRequestAddress = payload.repack_info().repack_request_address();
              ri.fSeq = payload.repack_info().fseq();
              ri.forceDisabledTape = payload.repack_info().force_disabled_tape();
            }
            // TODO serialization of payload maybe not necessary
            oh.set_payload(payload.SerializeAsString());
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
// RetrieveRequest::AsyncJobOwnerUpdater::getRepackInfo()
//------------------------------------------------------------------------------
const RetrieveRequest::RepackInfo& RetrieveRequest::AsyncJobOwnerUpdater::getRepackInfo() {
  return m_repackInfo;
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::getActivity()
//------------------------------------------------------------------------------
const optional<std::string>& RetrieveRequest::AsyncJobOwnerUpdater::getActivity() {
  return m_activity;
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::getDiskSystemName()
//------------------------------------------------------------------------------
const optional<std::string>& RetrieveRequest::AsyncJobOwnerUpdater::getDiskSystemName() {
  return m_diskSystemName;
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobOwnerUpdater::getRetrieveRequest()
//------------------------------------------------------------------------------
const common::dataStructures::RetrieveRequest& RetrieveRequest::AsyncJobOwnerUpdater::getRetrieveRequest() {
  return m_retrieveRequest;
}

cta::common::dataStructures::LifecycleTimings RetrieveRequest::getLifecycleTimings(){
  checkPayloadReadable();
  LifecycleTimingsSerDeser serDeser;
  serDeser.deserialize(m_payload.lifecycle_timings());
  return serDeser;
}

void RetrieveRequest::setCreationTime(const uint64_t creationTime){
  checkPayloadWritable();
  m_payload.mutable_lifecycle_timings()->set_creation_time(creationTime);
}

uint64_t RetrieveRequest::getCreationTime() {
  checkPayloadReadable();
  return m_payload.lifecycle_timings().creation_time();
}

void RetrieveRequest::setFirstSelectedTime(const uint64_t firstSelectedTime){
  checkPayloadWritable();
  m_payload.mutable_lifecycle_timings()->set_first_selected_time(firstSelectedTime);
}

void RetrieveRequest::setCompletedTime(const uint64_t completedTime){
  checkPayloadWritable();
  m_payload.mutable_lifecycle_timings()->set_completed_time(completedTime);
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
// RetrieveRequest::setFailed()
//------------------------------------------------------------------------------
void RetrieveRequest::setFailed() {
  checkPayloadWritable();
  m_payload.set_isfailed(true);
}

//------------------------------------------------------------------------------
// RetrieveRequest::isFailed()
//------------------------------------------------------------------------------
bool RetrieveRequest::isFailed() {
  checkPayloadReadable();
  return m_payload.isfailed();
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
// RetrieveRequest::asyncReportSucceedForRepack()
//------------------------------------------------------------------------------
RetrieveRequest::AsyncJobSucceedForRepackReporter * RetrieveRequest::asyncReportSucceedForRepack(uint32_t copyNb)
{
  std::unique_ptr<AsyncJobSucceedForRepackReporter> ret(new AsyncJobSucceedForRepackReporter);
  ret->m_updaterCallback = [&ret,copyNb](const std::string &in)->std::string{
        // We have a locked and fetched object, so we just need to work on its representation.
        cta::objectstore::serializers::ObjectHeader oh;
        if (!oh.ParseFromString(in)) {
          // Use a the tolerant parser to assess the situation.
          oh.ParsePartialFromString(in);
          throw cta::exception::Exception(std::string("In RetrieveRequest::asyncReportSucceedForRepack(): could not parse header: ")+
            oh.InitializationErrorString());
        }
        if (oh.type() != serializers::ObjectType::RetrieveRequest_t) {
          std::stringstream err;
          err << "In RetrieveRequest::asyncReportSucceedForRepack()::lambda(): wrong object type: " << oh.type();
          throw cta::exception::Exception(err.str());
        }
        serializers::RetrieveRequest payload;

        if (!payload.ParseFromString(oh.payload())) {
          // Use a the tolerant parser to assess the situation.
          payload.ParsePartialFromString(oh.payload());
          throw cta::exception::Exception(std::string("In RetrieveRequest::asyncReportSucceedForRepack(): could not parse payload: ")+
            payload.InitializationErrorString());
        }
        //payload.set_status(osdbJob->selectedCopyNb,serializers::RetrieveJobStatus::RJS_Succeeded);
        auto retrieveJobs = payload.mutable_jobs();
        for(auto &job : *retrieveJobs){
          if(job.copynb() == copyNb)
          {
            //Change the status to RJS_Succeed
            job.set_status(serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
            oh.set_payload(payload.SerializeAsString());
            return oh.SerializeAsString();
          }
        }
        ret->m_MountPolicy.deserialize(payload.mountpolicy());
        throw cta::exception::Exception("In RetrieveRequest::asyncReportSucceedForRepack::lambda(): copyNb not found");
    };
    ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(),ret->m_updaterCallback));
    return ret.release();
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncJobSucceedForRepackReporter::wait()
//------------------------------------------------------------------------------
void RetrieveRequest::AsyncJobSucceedForRepackReporter::wait(){
  m_backendUpdater->wait();
}

//------------------------------------------------------------------------------
// RetrieveRequest::asyncTransformToArchiveRequest()
//------------------------------------------------------------------------------
RetrieveRequest::AsyncRetrieveToArchiveTransformer * RetrieveRequest::asyncTransformToArchiveRequest(AgentReference& processAgent){
  std::unique_ptr<AsyncRetrieveToArchiveTransformer> ret(new AsyncRetrieveToArchiveTransformer);
  std::string processAgentAddress = processAgent.getAgentAddress();
  ret->m_updaterCallback = [processAgentAddress](const std::string &in)->std::string{
    // We have a locked and fetched object, so we just need to work on its representation.
    cta::objectstore::serializers::ObjectHeader oh;
    if (!oh.ParseFromString(in)) {
      // Use a the tolerant parser to assess the situation.
      oh.ParsePartialFromString(in);
      throw cta::exception::Exception(std::string("In RetrieveRequest::asyncTransformToArchiveRequest(): could not parse header: ")+
        oh.InitializationErrorString());
    }
    if (oh.type() != serializers::ObjectType::RetrieveRequest_t) {
      std::stringstream err;
      err << "In RetrieveRequest::asyncTransformToArchiveRequest()::lambda(): wrong object type: " << oh.type();
      throw cta::exception::Exception(err.str());
    }
    serializers::RetrieveRequest retrieveRequestPayload;

    if (!retrieveRequestPayload.ParseFromString(oh.payload())) {
      // Use a the tolerant parser to assess the situation.
      retrieveRequestPayload.ParsePartialFromString(oh.payload());
      throw cta::exception::Exception(std::string("In RetrieveRequest::asyncTransformToArchiveRequest(): could not parse payload: ")+
        retrieveRequestPayload.InitializationErrorString());
    }

    // Create the archive request from the RetrieveRequest
    serializers::ArchiveRequest archiveRequestPayload;
    const cta::objectstore::serializers::ArchiveFile& archiveFile = retrieveRequestPayload.archivefile();
    archiveRequestPayload.set_archivefileid(archiveFile.archivefileid());
    archiveRequestPayload.set_checksumblob(archiveFile.checksumblob());
    archiveRequestPayload.set_creationtime(archiveFile.creationtime());//This is the ArchiveFile creation time
    archiveRequestPayload.set_diskfileid(archiveFile.diskfileid());
    archiveRequestPayload.set_diskinstance(archiveFile.diskinstance());
    archiveRequestPayload.set_filesize(archiveFile.filesize());
    archiveRequestPayload.set_reconcilationtime(archiveFile.reconciliationtime());
    archiveRequestPayload.set_storageclass(archiveFile.storageclass());
    archiveRequestPayload.set_archiveerrorreporturl("");//No archive error report URL
    archiveRequestPayload.set_archivereporturl("");//No archive report URL
    archiveRequestPayload.set_reportdecided(false);

    // Convert/transfer the repack info.
    archiveRequestPayload.set_isrepack(true);
    ArchiveRequest::RepackInfoSerDeser archiveRepackInfoSerDeser;
    RetrieveRequest::RepackInfoSerDeser retrieveRepackInfoSerDeser;
    retrieveRepackInfoSerDeser.deserialize(retrieveRequestPayload.repack_info());
    archiveRepackInfoSerDeser.fSeq = retrieveRepackInfoSerDeser.fSeq;
    archiveRepackInfoSerDeser.fileBufferURL = retrieveRepackInfoSerDeser.fileBufferURL;
    archiveRepackInfoSerDeser.isRepack = true;
    archiveRepackInfoSerDeser.repackRequestAddress = retrieveRepackInfoSerDeser.repackRequestAddress;
    archiveRequestPayload.set_isrepack(true);
    archiveRepackInfoSerDeser.serialize(*archiveRequestPayload.mutable_repack_info());

    // Copy disk file informations into the new ArchiveRequest
    cta::objectstore::serializers::DiskFileInfo *archiveRequestDFI = archiveRequestPayload.mutable_diskfileinfo();
    archiveRequestDFI->CopyFrom(archiveFile.diskfileinfo());

    //ArchiveRequest source url is the same as the retrieveRequest destination URL
    const cta::objectstore::serializers::SchedulerRetrieveRequest schedulerRetrieveRequest = retrieveRequestPayload.schedulerrequest();
    archiveRequestPayload.set_srcurl(schedulerRetrieveRequest.dsturl());
    cta::objectstore::serializers::RequesterIdentity *archiveRequestUser = archiveRequestPayload.mutable_requester();
    archiveRequestUser->CopyFrom(schedulerRetrieveRequest.requester());

    //Copy the RetrieveRequest MountPolicy into the new ArchiveRequest
    cta::objectstore::serializers::MountPolicy *archiveRequestMP = archiveRequestPayload.mutable_mountpolicy();
    const cta::objectstore::serializers::MountPolicy& retrieveRequestMP = retrieveRequestPayload.mountpolicy();
    archiveRequestMP->CopyFrom(retrieveRequestMP);
    archiveRequestPayload.set_mountpolicyname(retrieveRequestMP.name());

    //Creation log is used by the queueing: job start time = archiveRequest creationLog.time
    cta::objectstore::serializers::EntryLog *archiveRequestCL = archiveRequestPayload.mutable_creationlog();
    archiveRequestCL->CopyFrom(retrieveRequestMP.creationlog());
    archiveRequestCL->set_host(cta::utils::getShortHostname());
    //Set the request creation time to now
    archiveRequestCL->set_time(time(nullptr));
    //Create archive jobs for each copyNb ro rearchive
    RetrieveRequest::RepackInfoSerDeser repackInfoSerDeser;
    repackInfoSerDeser.deserialize(retrieveRequestPayload.repack_info());
    // TODO: for the moment we just clone the retrieve request's policy.
    auto maxRetriesWithinMount = retrieveRequestPayload.jobs(0).maxretrieswithinmount();
    auto maxTotalRetries = retrieveRequestPayload.jobs(0).maxtotalretries();
    auto maxReportRetries = retrieveRequestPayload.jobs(0).maxreportretries();
    for(auto cntr: repackInfoSerDeser.copyNbsToRearchive) {
      auto *archiveJob = archiveRequestPayload.add_jobs();
      archiveJob->set_status(cta::objectstore::serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
      archiveJob->set_copynb(cntr);
      archiveJob->set_archivequeueaddress("");
      archiveJob->set_totalreportretries(0);
      archiveJob->set_lastmountwithfailure(0);
      archiveJob->set_totalretries(0);
      archiveJob->set_retrieswithinmount(0);
      archiveJob->set_maxretrieswithinmount(maxRetriesWithinMount);
      archiveJob->set_totalreportretries(0);
      archiveJob->set_maxtotalretries(maxTotalRetries);
      archiveJob->set_maxreportretries(maxReportRetries);
      archiveJob->set_tapepool(repackInfoSerDeser.archiveRouteMap[cntr]);
      archiveJob->set_owner(processAgentAddress);
    }
    //Serialize the new ArchiveRequest so that it replaces the RetrieveRequest
    oh.set_payload(archiveRequestPayload.SerializeAsString());
    //Change the type of the RetrieveRequest to ArchiveRequest
    oh.set_type(serializers::ObjectType::ArchiveRequest_t);
    //the new ArchiveRequest is now owned by the old RetrieveRequest owner (The Repack Request)
    oh.set_owner(oh.owner());

    return oh.SerializeAsString();
  };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(),ret->m_updaterCallback));
  return ret.release();
}

//------------------------------------------------------------------------------
// RetrieveRequest::AsyncRetrieveToArchiveTransformer::wait()
//------------------------------------------------------------------------------
void RetrieveRequest::AsyncRetrieveToArchiveTransformer::wait(){
  m_backendUpdater->wait();
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
// RetrieveRequest::getReportFailures()
//------------------------------------------------------------------------------
std::list<std::string> RetrieveRequest::getReportFailures() {
  checkPayloadReadable();
  std::list<std::string> ret;
  for (auto &j: m_payload.jobs()) {
    for (auto &f: j.reportfailurelogs()) {
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
void RetrieveRequest::setJobStatus(uint32_t copyNumber, const serializers::RetrieveJobStatus& status) {
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
