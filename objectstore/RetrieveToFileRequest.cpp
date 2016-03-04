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

#include "RetrieveToFileRequest.hpp"
#include "GenericObject.hpp"
#include "CreationLog.hpp"
#include "objectstore/cta.pb.h"
#include <json-c/json.h>

cta::objectstore::RetrieveToFileRequest::RetrieveToFileRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::RetrieveToFileRequest, serializers::RetrieveToFileRequest_t>(os, address) { }

cta::objectstore::RetrieveToFileRequest::RetrieveToFileRequest(GenericObject& go):
  ObjectOps<serializers::RetrieveToFileRequest, serializers::RetrieveToFileRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::RetrieveToFileRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveToFileRequest, serializers::RetrieveToFileRequest_t>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void cta::objectstore::RetrieveToFileRequest::addJob(const cta::TapeFileLocation & tapeFileLocation,
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

bool cta::objectstore::RetrieveToFileRequest::setJobSuccessful(uint16_t copyNumber) {
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
  throw NoSuchJob("In RetrieveToFileRequest::setJobSuccessful(): job not found");
}


void cta::objectstore::RetrieveToFileRequest::setArchiveFile(
  const cta::common::archiveNS::ArchiveFile& archiveFile) {
  checkPayloadWritable();
  auto *af = m_payload.mutable_archivefile();
  af->set_checksum(archiveFile.checksum);
  af->set_fileid(archiveFile.fileId);
  af->set_lastmodificationtime(archiveFile.lastModificationTime);
  af->set_nshostname(archiveFile.nsHostName);
  af->set_path(archiveFile.path);
  af->set_size(archiveFile.size);
}

cta::common::archiveNS::ArchiveFile cta::objectstore::RetrieveToFileRequest::getArchiveFile() {
  checkPayloadReadable();
  auto checksum = m_payload.archivefile().checksum();
  auto fileId = m_payload.archivefile().fileid();
  const time_t lastModificationTime = m_payload.archivefile().lastmodificationtime();
  auto nsHostName = m_payload.archivefile().nshostname();
  auto path = m_payload.archivefile().path();
  auto size = m_payload.archivefile().size();
  return common::archiveNS::ArchiveFile{path, nsHostName, fileId, size, checksum, lastModificationTime};
}

void cta::objectstore::RetrieveToFileRequest::setRemoteFile(
  const std::string& remoteFile) {
  checkPayloadWritable();
  m_payload.set_remotefile(remoteFile);
}

std::string cta::objectstore::RetrieveToFileRequest::getRemoteFile() {
  checkPayloadReadable();
  return m_payload.remotefile();
}

void cta::objectstore::RetrieveToFileRequest::setPriority(uint64_t priority) {
  checkPayloadWritable();
  m_payload.set_priority(priority);
}

uint64_t cta::objectstore::RetrieveToFileRequest::getPriority() {
  checkPayloadReadable();
  return m_payload.priority();
}

void cta::objectstore::RetrieveToFileRequest::setCreationLog(
  const objectstore::CreationLog& creationLog) {
  checkPayloadWritable();
  creationLog.serialize(*m_payload.mutable_log());
}

auto cta::objectstore::RetrieveToFileRequest::getCreationLog() -> CreationLog {
  checkPayloadReadable();
  CreationLog ret;
  ret.deserialize(m_payload.log());
  return ret;
}

void cta::objectstore::RetrieveToFileRequest::setRetrieveToDirRequestAddress(
  const std::string& dirRequestAddress) {
  checkPayloadWritable();
  m_payload.set_retrievetodiraddress(dirRequestAddress);
}

auto cta::objectstore::RetrieveToFileRequest::dumpJobs() -> std::list<JobDump> {
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

uint64_t cta::objectstore::RetrieveToFileRequest::getSize() {
  checkPayloadWritable();
  return m_payload.size();
}

void cta::objectstore::RetrieveToFileRequest::setSize(uint64_t size) {
  checkPayloadWritable();
  m_payload.set_size(size);
}

auto  cta::objectstore::RetrieveToFileRequest::getJob(uint16_t copyNb) -> JobDump {
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
  throw NoSuchJob("In objectstore::RetrieveToFileRequest::getJob(): job not found for this copyNb");
}

std::string cta::objectstore::RetrieveToFileRequest::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "RetrieveToFileRequest" << std::endl;
  struct json_object * jo = json_object_new_object();
  json_object_object_add(jo, "retrievetodiraddress", json_object_new_string(m_payload.retrievetodiraddress().c_str()));
  json_object_object_add(jo, "priority", json_object_new_int64(m_payload.priority()));
  json_object_object_add(jo, "size", json_object_new_int64(m_payload.size()));
  // Object for archive file
  json_object * jaf = json_object_new_object();
  json_object_object_add(jaf, "checksum", json_object_new_int(m_payload.archivefile().checksum()));
  json_object_object_add(jaf, "fileid", json_object_new_int(m_payload.archivefile().fileid()));
  json_object_object_add(jaf, "lastmodificationtime", json_object_new_int64(m_payload.archivefile().lastmodificationtime()));
  json_object_object_add(jaf, "nshostname", json_object_new_string(m_payload.archivefile().nshostname().c_str()));
  json_object_object_add(jaf, "path", json_object_new_string(m_payload.archivefile().path().c_str()));
  json_object_object_add(jaf, "size", json_object_new_int64(m_payload.archivefile().size()));
  json_object_object_add(jo, "archiveFile", jaf);
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
  // Object for log
  json_object * jlog = json_object_new_object();
  json_object_object_add(jlog, "comment", json_object_new_string(m_payload.log().comment().c_str()));
  json_object_object_add(jlog, "host", json_object_new_string(m_payload.log().host().c_str()));
  json_object_object_add(jlog, "time", json_object_new_int64(m_payload.log().time()));
  json_object_object_add(jo, "log", jlog);
  json_object_object_add(jo, "remoteFile", json_object_new_string(m_payload.remotefile().c_str()));
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}
