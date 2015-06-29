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

cta::objectstore::RetrieveToFileRequest::RetrieveToFileRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::RetrieveToFileRequest>(os, address) { }

cta::objectstore::RetrieveToFileRequest::RetrieveToFileRequest(GenericObject& go):
  ObjectOps<serializers::RetrieveToFileRequest>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::RetrieveToFileRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::RetrieveToFileRequest>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void cta::objectstore::RetrieveToFileRequest::addJob(uint16_t copyNumber,
  const std::string& tape, const std::string& tapeaddress) {
  checkPayloadWritable();
  auto *j = m_payload.add_jobs();
  j->set_copynb(copyNumber);
  j->set_status(serializers::RetrieveJobStatus::RJS_LinkingToTape);
  j->set_tape(tape);
  j->set_tapeaddress(tapeaddress);
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
}

void cta::objectstore::RetrieveToFileRequest::setArchiveFile(
  const std::string& archiveFile) {
  checkPayloadWritable();
  m_payload.set_archivefile(archiveFile);
}

void cta::objectstore::RetrieveToFileRequest::setRemoteFile(
  const std::string& remoteFile) {
  checkHeaderReadable();
  m_payload.set_remotefile(remoteFile);
}

void cta::objectstore::RetrieveToFileRequest::setPriority(uint64_t priority) {
  checkPayloadWritable();
  m_payload.set_priority(priority);
}

void cta::objectstore::RetrieveToFileRequest::setCreationLog(
  const objectstore::CreationLog& creationLog) {
  checkPayloadReadable();
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