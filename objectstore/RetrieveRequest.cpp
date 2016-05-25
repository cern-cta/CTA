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
#include "EntryLog.hpp"
#include "objectstore/cta.pb.h"
#include <json-c/json.h>

cta::objectstore::RetrieveRequest::RetrieveRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(os, address) { }

cta::objectstore::RetrieveRequest::RetrieveRequest(GenericObject& go):
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::RetrieveRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void cta::objectstore::RetrieveRequest::addJob(const cta::TapeFileLocation & tapeFileLocation,
    const std::string& tapeaddress) {
  checkPayloadWritable();
  auto *j = m_payload.add_jobs();
  j->set_copynb(tapeFileLocation.copyNb);
  j->set_status(serializers::RetrieveJobStatus::RJS_LinkingToTape);
  j->set_tape(tapeFileLocation.vid);
  j->set_tapeaddress(tapeaddress);
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
  j->set_blockid(tapeFileLocation.blockId);
  j->set_fseq(tapeFileLocation.fSeq);
}

bool cta::objectstore::RetrieveRequest::setJobSuccessful(uint16_t copyNumber) {
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
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setArchiveFileID(const uint64_t archiveFileID) {
  checkPayloadWritable();
  m_payload.set_archivefileid(archiveFileID);
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
uint64_t cta::objectstore::RetrieveRequest::getArchiveFileID() {
  checkPayloadReadable();
  return m_payload.archivefileid();
}

//------------------------------------------------------------------------------
// setDiskpoolName
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setDiskpoolName(const std::string &diskpoolName) {
  checkPayloadWritable();
  m_payload.set_diskpoolname(diskpoolName);
}

//------------------------------------------------------------------------------
// getDiskpoolName
//------------------------------------------------------------------------------
std::string cta::objectstore::RetrieveRequest::getDiskpoolName() {
  checkPayloadReadable();
  return m_payload.diskpoolname();
}

//------------------------------------------------------------------------------
// setDiskpoolThroughput
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setDiskpoolThroughput(const uint64_t diskpoolThroughput) {
  checkPayloadWritable();
  m_payload.set_diskpoolthroughput(diskpoolThroughput);
}

//------------------------------------------------------------------------------
// getDiskpoolThroughput
//------------------------------------------------------------------------------
uint64_t cta::objectstore::RetrieveRequest::getDiskpoolThroughput() {
  checkPayloadReadable();
  return m_payload.diskpoolthroughput();
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setDrData(const cta::common::dataStructures::DRData &drData) {
  checkPayloadWritable();
  auto payloadDrData = m_payload.mutable_drdata();
  payloadDrData->set_drblob(drData.drBlob);
  payloadDrData->set_drgroup(drData.drGroup);
  payloadDrData->set_drowner(drData.drOwner);
  payloadDrData->set_drpath(drData.drPath);
}

//------------------------------------------------------------------------------
// getDrData
//------------------------------------------------------------------------------
cta::common::dataStructures::DRData cta::objectstore::RetrieveRequest::getDrData() {
  checkPayloadReadable();
  cta::common::dataStructures::DRData drData;
  auto payloadDrData = m_payload.drdata();
  drData.drBlob=payloadDrData.drblob();
  drData.drGroup=payloadDrData.drgroup();
  drData.drOwner=payloadDrData.drowner();
  drData.drPath=payloadDrData.drpath();
  return drData;
}

//------------------------------------------------------------------------------
// setDstURL
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setDstURL(const std::string &dstURL) {
  checkPayloadWritable();
  m_payload.set_dsturl(dstURL);
}

//------------------------------------------------------------------------------
// getDstURL
//------------------------------------------------------------------------------
std::string cta::objectstore::RetrieveRequest::getDstURL() {
  checkPayloadReadable();
  return m_payload.dsturl();
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setRequester(const cta::common::dataStructures::UserIdentity &requester) {
  checkPayloadWritable();
  auto payloadRequester = m_payload.mutable_requester();
  payloadRequester->set_name(requester.name);
  payloadRequester->set_group(requester.group);
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity cta::objectstore::RetrieveRequest::getRequester() {
  checkPayloadReadable();
  cta::common::dataStructures::UserIdentity requester;
  auto payloadRequester = m_payload.requester();
  requester.name=payloadRequester.name();
  requester.group=payloadRequester.group();
  return requester;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::objectstore::RetrieveRequest::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  checkPayloadWritable();
  auto payloadCreationLog = m_payload.mutable_creationlog();
  payloadCreationLog->set_time(creationLog.time);
  payloadCreationLog->set_host(creationLog.host);
  payloadCreationLog->set_name(creationLog.user.name);
  payloadCreationLog->set_group(creationLog.user.group);
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::objectstore::RetrieveRequest::getCreationLog() {
  checkPayloadReadable();
  cta::common::dataStructures::EntryLog creationLog;
  cta::common::dataStructures::UserIdentity user;
  auto payloadCreationLog = m_payload.creationlog();
  user.name=payloadCreationLog.name();
  user.group=payloadCreationLog.group();
  creationLog.user=user;
  creationLog.host=payloadCreationLog.host();
  creationLog.time=payloadCreationLog.time();
  return creationLog;  
}

auto cta::objectstore::RetrieveRequest::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    ret.push_back(JobDump());
    ret.back().copyNb = j->copynb();
    ret.back().tape = j->tape();
    ret.back().tapeAddress = j->tapeaddress();
  }
  return ret;
}

auto  cta::objectstore::RetrieveRequest::getJob(uint16_t copyNb) -> JobDump {
  checkPayloadReadable();
  // find the job
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    if (j->copynb() == copyNb) {
      JobDump ret;
      ret.blockid = j->blockid();
      ret.copyNb = j->copynb();
      ret.fseq = j->fseq();
      ret.tape = j->tape();
      ret.tapeAddress = j->tapeaddress();
      return ret;
    }
  }
  throw NoSuchJob("In objectstore::RetrieveRequest::getJob(): job not found for this copyNb");
}

std::string cta::objectstore::RetrieveRequest::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "RetrieveRequest" << std::endl;
  struct json_object * jo = json_object_new_object();
  json_object_object_add(jo, "archivefileid", json_object_new_int64(m_payload.archivefileid()));
  json_object_object_add(jo, "dsturl", json_object_new_string(m_payload.dsturl().c_str()));
  json_object_object_add(jo, "diskpoolname", json_object_new_string(m_payload.diskpoolname().c_str()));
  json_object_object_add(jo, "diskpoolthroughput", json_object_new_int64(m_payload.diskpoolthroughput()));
  // Object for creation log
  json_object * jaf = json_object_new_object();
  json_object_object_add(jaf, "host", json_object_new_string(m_payload.creationlog().host().c_str()));
  json_object_object_add(jaf, "time", json_object_new_int64(m_payload.creationlog().time()));
  json_object_object_add(jaf, "name", json_object_new_string(m_payload.creationlog().name().c_str()));
  json_object_object_add(jaf, "group", json_object_new_string(m_payload.creationlog().group().c_str()));
  json_object_object_add(jo, "creationlog", jaf);
  // Array for jobs
  json_object * jja = json_object_new_array();
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    // Object for job
    json_object * jj = json_object_new_object();
    
    json_object_object_add(jj, "copynb", json_object_new_int(j->copynb()));
    json_object_object_add(jj, "retrieswithinmount", json_object_new_int(j->retrieswithinmount()));
    json_object_object_add(jj, "totalretries", json_object_new_int(j->totalretries()));
    json_object_object_add(jj, "status", json_object_new_int64(j->status()));
    json_object_object_add(jj, "fseq", json_object_new_int64(j->fseq()));
    json_object_object_add(jj, "blockid", json_object_new_int64(j->blockid()));
    json_object_object_add(jj, "tape", json_object_new_string(j->tape().c_str()));
    json_object_object_add(jj, "tapeAddress", json_object_new_string(j->tapeaddress().c_str()));
    json_object_array_add(jja, jj);
  }
  json_object_object_add(jo, "jobs", jja);
  // Object for drdata
  json_object * jlog = json_object_new_object();
  json_object_object_add(jlog, "drblob", json_object_new_string(m_payload.drdata().drblob().c_str()));
  json_object_object_add(jlog, "drgroup", json_object_new_string(m_payload.drdata().drgroup().c_str()));
  json_object_object_add(jlog, "drowner", json_object_new_string(m_payload.drdata().drowner().c_str()));
  json_object_object_add(jlog, "drpath", json_object_new_string(m_payload.drdata().drpath().c_str()));
  json_object_object_add(jo, "drdata", jlog);
  // Object for requester
  json_object * jrf = json_object_new_object();
  json_object_object_add(jrf, "name", json_object_new_string(m_payload.requester().name().c_str()));
  json_object_object_add(jrf, "group", json_object_new_string(m_payload.requester().group().c_str()));
  json_object_object_add(jo, "requester", jrf);
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}

