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
// initialize
//------------------------------------------------------------------------------
void RetrieveRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

//------------------------------------------------------------------------------
// garbageCollect
//------------------------------------------------------------------------------
void RetrieveRequest::garbageCollect(const std::string& presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
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
  Helpers::getLockedAndFetchedQueue<RetrieveQueue>(rq, rql, agentReference, bestVid);
  // Enqueue add the job to the queue
  objectstore::MountPolicySerDeser mp;
  mp.deserialize(m_payload.mountpolicy());
  rq.addJobIfNecessary(bestTapeFile->copynb(), bestTapeFile->fseq(), getAddressIfSet(), m_payload.archivefile().filesize(), 
    mp, m_payload.schedulerrequest().entrylog().time());
  auto jobsSummary=rq.getJobsSummary();
  rq.commit();
  // We can now make the transition official
  bestJob->set_status(serializers::RetrieveJobStatus::RJS_Pending);
  m_payload.set_activecopynb(bestJob->copynb());
  setOwner(rq.getAddressIfSet());
  commit();
  {
    log::ScopedParamContainer params(lc);
    params.add("jobObject", getAddressIfSet())
          .add("queueObject", rq.getAddressIfSet())
          .add("copynb", bestTapeFile->copynb())
          .add("vid", bestTapeFile->vid());
    lc.log(log::INFO, "In RetrieveRequest::garbageCollect(): requeued the request.");
  }
  Helpers::updateRetrieveQueueStatisticsCache(bestVid, jobsSummary.files, jobsSummary.bytes, jobsSummary.priority);
}

//------------------------------------------------------------------------------
// setJobSuccessful
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
// setSchedulerRequest
//------------------------------------------------------------------------------
void RetrieveRequest::setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest& retrieveRequest) {
  checkPayloadWritable();
  auto *sr = m_payload.mutable_schedulerrequest();
  sr->mutable_requester()->set_name(retrieveRequest.requester.name);
  sr->mutable_requester()->set_group(retrieveRequest.requester.group);
  sr->set_archivefileid(retrieveRequest.archiveFileID);
  sr->set_dsturl(retrieveRequest.dstURL);
  DiskFileInfoSerDeser dfisd(retrieveRequest.diskFileInfo);
  dfisd.serialize(*sr->mutable_diskfileinfo());
  objectstore::EntryLogSerDeser el(retrieveRequest.creationLog);
  el.serialize(*sr->mutable_entrylog());
}

//------------------------------------------------------------------------------
// getSchedulerRequest
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
  objectstore::DiskFileInfoSerDeser dfisd;
  dfisd.deserialize(m_payload.schedulerrequest().diskfileinfo());
  ret.diskFileInfo = dfisd;
  return ret;
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------

cta::common::dataStructures::ArchiveFile RetrieveRequest::getArchiveFile() {
  objectstore::ArchiveFileSerDeser af;
  af.deserialize(m_payload.archivefile());
  return af;
}


//------------------------------------------------------------------------------
// setRetrieveFileQueueCriteria
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
// dumpJobs
//------------------------------------------------------------------------------
auto RetrieveRequest::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  for (auto & j: m_payload.jobs()) {
    ret.push_back(JobDump());
    ret.back().copyNb=j.copynb();
    ret.back().maxRetriesWithinMount=j.maxretrieswithinmount();
    ret.back().maxTotalRetries=j.maxtotalretries();
    ret.back().retriesWithinMount=j.retrieswithinmount();
    ret.back().totalRetries=j.totalretries();
    // TODO: status
  }
  return ret;
}

//------------------------------------------------------------------------------
// getJob
//------------------------------------------------------------------------------
auto  RetrieveRequest::getJob(uint16_t copyNb) -> JobDump {
  checkPayloadReadable();
  // find the job
  for (auto & j: m_payload.jobs()) {
    if (j.copynb()==copyNb) {
      JobDump ret;
      ret.copyNb=copyNb;
      ret.maxRetriesWithinMount=j.maxretrieswithinmount();
      ret.maxTotalRetries=j.maxtotalretries();
      ret.retriesWithinMount=j.retrieswithinmount();
      ret.totalRetries=j.totalretries();
      return ret;
    }
  }
  throw NoSuchJob("In objectstore::RetrieveRequest::getJob(): job not found for this copyNb");
}

auto RetrieveRequest::getJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  for (auto & j: m_payload.jobs()) {
    ret.push_back(JobDump());
    ret.back().copyNb=j.copynb();
    ret.back().maxRetriesWithinMount=j.maxretrieswithinmount();
    ret.back().maxTotalRetries=j.maxtotalretries();
    ret.back().retriesWithinMount=j.retrieswithinmount();
    ret.back().totalRetries=j.totalretries();
  }
  return ret;
}

bool RetrieveRequest::addJobFailure(uint16_t copyNumber, uint64_t mountId) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  // Find the job and update the number of failures
  // (and return the full request status: failed (true) or to be retried (false))
  // The request will go through a full requeueing if retried (in caller).
  for (auto j: *jl) {
    if (j.copynb() == copyNumber) {
      if (j.lastmountwithfailure() == mountId) {
        j.set_retrieswithinmount(j.retrieswithinmount() + 1);
      } else {
        j.set_retrieswithinmount(1);
        j.set_lastmountwithfailure(mountId);
      }
      j.set_totalretries(j.totalretries() + 1);
    }
    if (j.totalretries() >= j.maxtotalretries()) {
      j.set_status(serializers::RJS_Failed);
      return finishIfNecessary();
    } else {
      j.set_status(serializers::RJS_Pending);
      return false;
    }
  }
  throw NoSuchJob ("In ArchiveRequest::addJobFailure(): could not find job");
}

bool RetrieveRequest::finishIfNecessary() {
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
  return true;
}

serializers::RetrieveJobStatus RetrieveRequest::getJobStatus(uint16_t copyNumber) {
  checkPayloadReadable();
  for (auto & j: m_payload.jobs())
    if (j.copynb() == copyNumber)
      return j.status();
  std::stringstream err;
  err << "In RetrieveRequest::getJobStatus(): could not find job for copynb=" << copyNumber;
  throw exception::Exception(err.str());
}

void RetrieveRequest::setActiveCopyNumber(uint32_t activeCopyNb) {
  checkPayloadWritable();
  m_payload.set_activecopynb(activeCopyNb);
}

uint32_t RetrieveRequest::getActiveCopyNumber() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

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

cta::common::dataStructures::EntryLog RetrieveRequest::getEntryLog() {
  checkPayloadReadable();
  EntryLogSerDeser el;
  el.deserialize(m_payload.schedulerrequest().entrylog());
  return el;
}




std::string RetrieveRequest::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

}} // namespace cta::objectstore

