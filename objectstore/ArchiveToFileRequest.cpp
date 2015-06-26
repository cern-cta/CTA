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

#include "ArchiveToFileRequest.hpp"
#include "GenericObject.hpp"
#include "CreationLog.hpp"

cta::objectstore::ArchiveToFileRequest::ArchiveToFileRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::ArchiveToFileRequest>(os, address) { }

cta::objectstore::ArchiveToFileRequest::ArchiveToFileRequest(GenericObject& go):
  ObjectOps<serializers::ArchiveToFileRequest>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::ArchiveToFileRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::ArchiveToFileRequest>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void cta::objectstore::ArchiveToFileRequest::addJob(uint16_t copyNumber,
  const std::string& tapepool, const std::string& tapepooladdress) {
  checkPayloadWritable();
  auto *j = m_payload.add_jobs();
  j->set_copynb(copyNumber);
  j->set_status(serializers::ArchiveJobStatus::AJS_LinkingToTapePool);
  j->set_tapepool(tapepool);
  j->set_tapepooladdress(tapepooladdress);
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
}

void cta::objectstore::ArchiveToFileRequest::setArchiveFile(
  const std::string& archiveFile) {
  checkPayloadWritable();
  m_payload.set_archivefile(archiveFile);
}

void cta::objectstore::ArchiveToFileRequest::setRemoteFile(
  const std::string& remoteFile) {
  checkHeaderReadable();
  m_payload.set_remotefile(remoteFile);
}

void cta::objectstore::ArchiveToFileRequest::setPriority(uint64_t priority) {
  checkPayloadWritable();
  m_payload.set_priority(priority);
}

void cta::objectstore::ArchiveToFileRequest::setLog(
  const objectstore::CreationLog& creationLog) {
  checkPayloadReadable();
  creationLog.serialize(*m_payload.mutable_log());
}

void cta::objectstore::ArchiveToFileRequest::setArchiveToDirRequestAddress(
  const std::string& dirRequestAddress) {
  checkPayloadWritable();
  m_payload.set_archivetodiraddress(dirRequestAddress);
}

auto cta::objectstore::ArchiveToFileRequest::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    ret.push_back(JobDump());
    ret.back().copyNb = j->copynb();
    ret.back().tapePool = j->tapepool();
    ret.back().tapePool = j->tapepooladdress();
  }
  return ret;
}

uint64_t cta::objectstore::ArchiveToFileRequest::getSize() {
  checkPayloadWritable();
  return m_payload.size();
}

void cta::objectstore::ArchiveToFileRequest::setSize(uint64_t size) {
  checkPayloadWritable();
  m_payload.set_size(size);
}



