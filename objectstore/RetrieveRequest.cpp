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
#include <json-c/json.h>

namespace cta { namespace objectstore {

RetrieveRequest::RetrieveRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(os, address) { }

RetrieveRequest::RetrieveRequest(GenericObject& go):
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void RetrieveRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void RetrieveRequest::garbageCollect(const std::string& presumedOwner) {
  throw cta::exception::Exception("In RetrieveRequest::garbageCollect(): not implemented.");
}

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
  objectstore::EntryLogSerDeser el(retrieveRequest.entryLog);
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
  objectstore::EntryLogSerDeser el(ret.entryLog);
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
  std::stringstream ret;
  ret << "RetrieveRequest" << std::endl;
  struct json_object * jo = json_object_new_object();
//  struct json_object * jaf = json_object_new_object();
  // TODO: fill up again
//  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().fileid()));
//  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().creationtime()));
//  json_object_object_add(jaf, "fileid", json_object_new_string(m_payload.archivefile().checksumtype().c_str()));
//  json_object_object_add(jaf, "fileid", json_object_new_string(m_payload.archivefile().checksumvalue().c_str()));
//  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().size()));
//  json_object_object_add(jo, "creationlog", jaf);
//  json_object_object_add(jo, "dsturl", json_object_new_string(m_payload.dsturl().c_str()));
//  // Object for creation log
//  json_object * jcl = json_object_new_object();
//  json_object_object_add(jcl, "host", json_object_new_string(m_payload.creationlog().host().c_str()));
//  json_object_object_add(jcl, "time", json_object_new_int64(m_payload.creationlog().time()));
//  json_object_object_add(jcl, "username", json_object_new_string(m_payload.creationlog().username().c_str()));
//  json_object_object_add(jo, "creationlog", jcl);
  // Array for jobs
  json_object * jja = json_object_new_array();
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    // Object for job
    json_object * jj = json_object_new_object();
    
    // TODO: fix
    //json_object_object_add(jj, "copynb", json_object_new_int(j->copynb()));
    json_object_object_add(jj, "retrieswithinmount", json_object_new_int(j->retrieswithinmount()));
    json_object_object_add(jj, "totalretries", json_object_new_int(j->totalretries()));
    json_object_object_add(jj, "status", json_object_new_int64(j->status()));
    //json_object_object_add(jj, "fseq", json_object_new_int64(j->fseq()));
    //json_object_object_add(jj, "blockid", json_object_new_int64(j->blockid()));
    //json_object_object_add(jj, "tape", json_object_new_string(j->tape().c_str()));
    //json_object_object_add(jj, "tapeAddress", json_object_new_string(j->tapeaddress().c_str()));
    json_object_array_add(jja, jj);
  }
  json_object_object_add(jo, "jobs", jja);
  // Object for diskfileinfo
//  json_object * jlog = json_object_new_object();
  // TODO: fill up again
//  json_object_object_add(jlog, "recoveryblob", json_object_new_string(m_payload.diskfileinfo().recoveryblob().c_str()));
//  json_object_object_add(jlog, "group", json_object_new_string(m_payload.diskfileinfo().group().c_str()));
//  json_object_object_add(jlog, "owner", json_object_new_string(m_payload.diskfileinfo().owner().c_str()));
//  json_object_object_add(jlog, "path", json_object_new_string(m_payload.diskfileinfo().path().c_str()));
//  json_object_object_add(jo, "diskfileinfo", jlog);
//  // Object for requester
//  json_object * jrf = json_object_new_object();
//  json_object_object_add(jrf, "name", json_object_new_string(m_payload.requester().name().c_str()));
//  json_object_object_add(jrf, "group", json_object_new_string(m_payload.requester().group().c_str()));
//  json_object_object_add(jo, "requester", jrf);
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

}} // namespace cta::objectstore

