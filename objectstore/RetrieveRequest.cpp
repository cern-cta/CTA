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

void RetrieveRequest::addJob(const cta::common::dataStructures::TapeFile & tapeFile,
    const std::string& tapeaddress) {
  checkPayloadWritable();
  auto *j = m_payload.add_jobs();
  j->set_copynb(tapeFile.copyNb);
  j->set_status(serializers::RetrieveJobStatus::RJS_LinkingToTape);
  j->set_tape(tapeFile.vid);
  j->set_tapeaddress(tapeaddress);
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
  j->set_blockid(tapeFile.blockId);
  j->set_fseq(tapeFile.fSeq);
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
// setArchiveFile
//------------------------------------------------------------------------------
void RetrieveRequest::setArchiveFile(const cta::common::dataStructures::ArchiveFile& archiveFile) {
  checkPayloadWritable();
  auto *af = m_payload.mutable_archivefile();
  af->set_checksumtype(archiveFile.checksumType);
  af->set_checksumvalue(archiveFile.checksumValue);
  af->set_fileid(archiveFile.archiveFileID);
  af->set_creationtime(archiveFile.creationTime);
  af->set_size(archiveFile.fileSize);
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------

cta::common::dataStructures::ArchiveFile RetrieveRequest::getArchiveFile() {
  checkPayloadReadable();
  common::dataStructures::ArchiveFile ret;
  ret.archiveFileID = m_payload.archivefile().fileid();
  ret.checksumType = m_payload.archivefile().checksumtype();
  ret.checksumValue = m_payload.archivefile().checksumvalue();
  ret.creationTime = m_payload.archivefile().creationtime();
  ret.diskFileId = m_payload.archivefile().diskfileid();
  ret.diskInstance = m_payload.diskinstance();
  ret.drData.drBlob = m_payload.drdata().drblob();
  ret.drData.drGroup = m_payload.drdata().drgroup();
  ret.drData.drOwner = m_payload.drdata().drowner();
  ret.drData.drPath = m_payload.drdata().drpath();
  ret.fileSize = m_payload.archivefile().size();
  ret.reconciliationTime = m_payload.reconcilationtime();
  ret.storageClass = m_payload.storageclass();
  for (auto & j: m_payload.jobs()) {
    ret.tapeFiles[j.copynb()].blockId = j.blockid();
    ret.tapeFiles[j.copynb()].compressedSize = j.compressedsize();
    ret.tapeFiles[j.copynb()].copyNb = j.copynb();
    ret.tapeFiles[j.copynb()].creationTime = j.creationtime();
    ret.tapeFiles[j.copynb()].fSeq = j.fseq();
    ret.tapeFiles[j.copynb()].vid = j.tape();
  }
  return ret;
}

//------------------------------------------------------------------------------
// setDiskpoolName
//------------------------------------------------------------------------------
void RetrieveRequest::setDiskpoolName(const std::string &diskpoolName) {
  checkPayloadWritable();
  m_payload.set_diskpoolname(diskpoolName);
}

//------------------------------------------------------------------------------
// getDiskpoolName
//------------------------------------------------------------------------------
std::string RetrieveRequest::getDiskpoolName() {
  checkPayloadReadable();
  return m_payload.diskpoolname();
}

//------------------------------------------------------------------------------
// setDiskpoolThroughput
//------------------------------------------------------------------------------
void RetrieveRequest::setDiskpoolThroughput(const uint64_t diskpoolThroughput) {
  checkPayloadWritable();
  m_payload.set_diskpoolthroughput(diskpoolThroughput);
}

//------------------------------------------------------------------------------
// getDiskpoolThroughput
//------------------------------------------------------------------------------
uint64_t RetrieveRequest::getDiskpoolThroughput() {
  checkPayloadReadable();
  return m_payload.diskpoolthroughput();
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void RetrieveRequest::setDrData(const cta::common::dataStructures::DRData &drData) {
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
cta::common::dataStructures::DRData RetrieveRequest::getDrData() {
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
void RetrieveRequest::setDstURL(const std::string &dstURL) {
  checkPayloadWritable();
  m_payload.set_dsturl(dstURL);
}

//------------------------------------------------------------------------------
// getDstURL
//------------------------------------------------------------------------------
std::string RetrieveRequest::getDstURL() {
  checkPayloadReadable();
  return m_payload.dsturl();
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void RetrieveRequest::setRequester(const cta::common::dataStructures::UserIdentity &requester) {
  checkPayloadWritable();
  auto payloadRequester = m_payload.mutable_requester();
  payloadRequester->set_name(requester.name);
  payloadRequester->set_group(requester.group);
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity RetrieveRequest::getRequester() {
  checkPayloadReadable();
  cta::common::dataStructures::UserIdentity requester;
  auto payloadRequester = m_payload.requester();
  requester.name=payloadRequester.name();
  requester.group=payloadRequester.group();
  return requester;
}

//------------------------------------------------------------------------------
// setEntryLog
//------------------------------------------------------------------------------

void RetrieveRequest::setEntryLog(const objectstore::EntryLog& entryLog) {
  checkPayloadWritable();
  auto payloadCreationLog = m_payload.mutable_creationlog();
  payloadCreationLog->set_time(entryLog.time);
  payloadCreationLog->set_host(entryLog.host);
  payloadCreationLog->set_username(entryLog.username);
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
objectstore::EntryLog RetrieveRequest::getEntryLog() {
  checkHeaderReadable();
  cta::common::dataStructures::EntryLog entryLog;
  auto payloadCreationLog = m_payload.creationlog();
  entryLog.username=payloadCreationLog.username();
  entryLog.host=payloadCreationLog.host();
  entryLog.time=payloadCreationLog.time();
  return entryLog;  
}

//------------------------------------------------------------------------------
// dumpJobs
//------------------------------------------------------------------------------
auto RetrieveRequest::dumpJobs() -> std::list<JobDump> {
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

auto  RetrieveRequest::getJob(uint16_t copyNb) -> JobDump {
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

std::string RetrieveRequest::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "RetrieveRequest" << std::endl;
  struct json_object * jo = json_object_new_object();
  struct json_object * jaf = json_object_new_object();
  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().fileid()));
  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().creationtime()));
  json_object_object_add(jaf, "fileid", json_object_new_string(m_payload.archivefile().checksumtype().c_str()));
  json_object_object_add(jaf, "fileid", json_object_new_string(m_payload.archivefile().checksumvalue().c_str()));
  json_object_object_add(jaf, "fileid", json_object_new_int64(m_payload.archivefile().size()));
  json_object_object_add(jo, "creationlog", jaf);
  json_object_object_add(jo, "dsturl", json_object_new_string(m_payload.dsturl().c_str()));
  json_object_object_add(jo, "diskpoolname", json_object_new_string(m_payload.diskpoolname().c_str()));
  json_object_object_add(jo, "diskpoolthroughput", json_object_new_int64(m_payload.diskpoolthroughput()));
  // Object for creation log
  json_object * jcl = json_object_new_object();
  json_object_object_add(jcl, "host", json_object_new_string(m_payload.creationlog().host().c_str()));
  json_object_object_add(jcl, "time", json_object_new_int64(m_payload.creationlog().time()));
  json_object_object_add(jcl, "username", json_object_new_string(m_payload.creationlog().username().c_str()));
  json_object_object_add(jo, "creationlog", jcl);
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

}} // namespace cta::objectstore

