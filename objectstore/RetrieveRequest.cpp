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
  if (getOwner() != presumedOwner) return;
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
  // If we have to fetch the status of the tapes and queued for the non-disabled vids.
  auto bestVid=Helpers::selectBestRetrieveQueue(candidateVids, catalogue, m_objectStore);
  throw cta::exception::Exception("In RetrieveRequest::garbageCollect(): not implemented.");
}

//------------------------------------------------------------------------------
// setJobSuccessful
//------------------------------------------------------------------------------
void RetrieveRequest::addJob(uint64_t copyNb, uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries) {
  checkPayloadWritable();
  auto *tf = m_payload.add_jobs();
  tf->set_copynb(copyNb);
  tf->set_maxretrieswithinmount(maxRetiesWithinMount);
  tf->set_maxtotalretries(maxTotalRetries);
  tf->set_retrieswithinmount(0);
  tf->set_totalretries(0);
  tf->set_status(serializers::RetrieveJobStatus::RJS_Pending);
}

//------------------------------------------------------------------------------
// setJobSuccessful
//------------------------------------------------------------------------------
bool RetrieveRequest::setJobSuccessful(uint16_t copyNumber) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    if (j->copynb() == copyNumber) {
      j->set_status(serializers::RetrieveJobStatus::RJS_Complete);
      for (auto j2=jl->begin(); j2!=jl->end(); j2++) {
        if (j2->status()!= serializers::RetrieveJobStatus::RJS_Complete &&
            j2->status()!= serializers::RetrieveJobStatus::RJS_Failed)
          return false;
      }
      return true;
    }
  }
  throw NoSuchJob("In RetrieveRequest::setJobSuccessful(): job not found");
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

RetrieveRequest::FailuresCount RetrieveRequest::addJobFailure(uint16_t copyNumber, uint64_t sessionId) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

void RetrieveRequest::finish() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

void RetrieveRequest::setActiveCopyNumber(uint32_t activeCopyNb) {
  checkPayloadWritable();
  m_payload.set_activecopynb(activeCopyNb);
}

uint32_t RetrieveRequest::getActiveCopyNumber() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

cta::common::dataStructures::RetrieveFileQueueCriteria RetrieveRequest::getRetrieveFileQueueCriteria() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
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

