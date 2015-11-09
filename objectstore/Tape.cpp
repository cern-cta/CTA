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

#include "Tape.hpp"
#include "GenericObject.hpp"
#include "CreationLog.hpp"

cta::objectstore::Tape::Tape(const std::string& address, Backend& os):
  ObjectOps<serializers::Tape>(os, address) { }

cta::objectstore::Tape::Tape(GenericObject& go):
  ObjectOps<serializers::Tape>(go.objectStore()){
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::Tape::initialize(const std::string &name, 
    const std::string &logicallibrary, const cta::CreationLog & creationLog) {
  ObjectOps<serializers::Tape>::initialize();
  // Set the reguired fields
  objectstore::CreationLog oscl(creationLog);
  oscl.serialize(*m_payload.mutable_log());
  m_payload.set_vid(name);
  m_payload.set_bytesstored(0);
  m_payload.set_lastfseq(0);
  m_payload.set_logicallibrary(logicallibrary);
  m_payload.set_oldestjobtime(0);
  m_payload.set_priority(0);
  m_payload.set_retrievejobstotalsize(0);
  m_payload.set_busy(false);
  m_payload.set_archived(false);
  m_payload.set_disabled(false);
  m_payload.set_readonly(false);
  m_payload.set_full(false);
  auto cm = m_payload.mutable_currentmount();
  cm->set_drivemodel("");
  cm->set_drivename("");
  cm->set_driveserial("");
  cm->set_drivevendor("");
  cm->set_host("");
  cm->set_time(0);
  m_payload.set_currentmounttype(serializers::MountType::Archive);
  m_payloadInterpreted = true;
}


bool cta::objectstore::Tape::isEmpty() {
  checkPayloadReadable();
  return !m_payload.retrievejobs_size();
}

void cta::objectstore::Tape::removeIfEmpty() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In Tape::removeIfEmpty: trying to remove an tape with retrieves queued");
  }
  remove();
}

cta::CreationLog cta::objectstore::Tape::getCreationLog() {
  checkPayloadReadable();
  objectstore::CreationLog oscl;
  oscl.deserialize(m_payload.log());
  return cta::CreationLog(oscl);
}

void cta::objectstore::Tape::setCreationLog(const cta::CreationLog& creationLog) {
  checkPayloadWritable();
  objectstore::CreationLog oscl(creationLog);
  oscl.serialize(*m_payload.mutable_log());
}

void cta::objectstore::Tape::setStoredData(uint64_t bytes) {
  checkPayloadWritable();
  m_payload.set_bytesstored(bytes);
}

void cta::objectstore::Tape::addStoredData(uint64_t bytes) {
  checkPayloadWritable();
  m_payload.set_bytesstored(m_payload.bytesstored()+bytes);
}

uint64_t cta::objectstore::Tape::getStoredData() {
  checkPayloadReadable();
  return m_payload.bytesstored();
}

void cta::objectstore::Tape::setLastFseq(uint64_t lastFseq) {
  checkPayloadWritable();
  m_payload.set_lastfseq(lastFseq);
}

uint64_t cta::objectstore::Tape::getLastFseq() {
  checkPayloadReadable();
  return m_payload.lastfseq();
}

std::string cta::objectstore::Tape::getVid() {
  checkPayloadReadable();
  return m_payload.vid();
}

std::string cta::objectstore::Tape::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< Tape dump start: vid=" << m_payload.vid() << std::endl;
  ret << "  lastFseq=" << m_payload.lastfseq() 
      << " bytesStored=" << m_payload.bytesstored() << std::endl;
  ret << "  Retrieve jobs queued: " << m_payload.retrievejobs_size() << std::endl;
  if (m_payload.readmounts_size()) {
    auto lrm = m_payload.readmounts(0);
    ret << "  Latest read for mount: " << lrm.host() << " " << lrm.time() << " " 
        << lrm.drivevendor() << " " << lrm.drivemodel() << " " 
        << lrm.driveserial() << std::endl;
  }
  if (m_payload.writemounts_size()) {
    auto lwm = m_payload.writemounts(0);
    ret << "  Latest write for mount: " << lwm.host() << " " << lwm.time() << " " 
        << lwm.drivevendor() << " " << lwm.drivemodel() << " " 
        << lwm.driveserial() << std::endl;
  }
  ret << ">>>> Tape dump end" << std::endl;
  return ret.str();
}

void cta::objectstore::Tape::addJob(const RetrieveToFileRequest::JobDump& job,
  const std::string & retrieveToFileAddress, uint64_t size, uint64_t priority,
  time_t startTime) {
  checkPayloadWritable();
  // Manage the cumulative properties
  if (m_payload.retrievejobs_size()) {
    if (priority > m_payload.priority()) {
      m_payload.set_priority(priority);
    }
    if (m_payload.oldestjobtime() > (uint64_t)startTime) {
      m_payload.set_oldestjobtime(startTime);
    }
    m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() + size);
  } else {
    m_payload.set_priority(priority);
    m_payload.set_oldestjobtime(startTime);
    m_payload.set_retrievejobstotalsize(size);
  }
  auto * j = m_payload.add_retrievejobs();
  j->set_address(retrieveToFileAddress);
  j->set_size(size);
  j->set_copynb(job.copyNb);
}

std::string cta::objectstore::Tape::getLogicalLibrary() {
  checkPayloadReadable();
  return m_payload.logicallibrary();
}

cta::objectstore::Tape::JobsSummary cta::objectstore::Tape::getJobsSummary() {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.retrievejobstotalsize();
  ret.files = m_payload.retrievejobs_size();
  ret.oldestJobStartTime = m_payload.oldestjobtime();
  ret.priority = m_payload.priority();
  return ret;
}

auto cta::objectstore::Tape::dumpAndFetchRetrieveRequests() 
  -> std::list<RetrieveRequestDump> {
  checkPayloadReadable();
  std::list<RetrieveRequestDump> ret;
  auto & rjl = m_payload.retrievejobs();
  for (auto rj=rjl.begin(); rj!=rjl.end(); rj++) {
    try {
      cta::objectstore::RetrieveToFileRequest rtfr(rj->address(),m_objectStore);
      objectstore::ScopedSharedLock rtfrl(rtfr);
      rtfr.fetch();
      ret.push_back(RetrieveRequestDump());
      auto & retReq = ret.back();
      retReq.archiveFile = rtfr.getArchiveFile();
      retReq.remoteFile = rtfr.getRemoteFile();
      retReq.creationLog = rtfr.getCreationLog();
      // Find the copy number from the list of jobs
      retReq.activeCopyNb = rj->copynb();
      auto jl = rtfr.dumpJobs();
      for (auto j=jl.begin(); j!= jl.end(); j++) {
        retReq.tapeCopies.push_back(TapeFileLocation());
        auto & retJob = retReq.tapeCopies.back();
        retJob.blockId = j->blockid;
        retJob.copyNb = j->copyNb;
        retJob.fSeq = j->fseq;
        retJob.vid = j->tape;
      }
    } catch (cta::exception::Exception &) {}
  }
  return ret;
}

auto cta::objectstore::Tape::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & rjl = m_payload.retrievejobs();
  for (auto rj=rjl.begin(); rj!=rjl.end(); rj++) {
    ret.push_back(JobDump());
    auto & b=ret.back();
    b.copyNb = rj->copynb();
    b.address = rj->address();
    b.size = rj->size();
  }
  return ret;
}


bool cta::objectstore::Tape::isArchived() {
  checkPayloadReadable();
  return m_payload.archived();
}

void cta::objectstore::Tape::setArchived(bool archived) {
  checkPayloadWritable();
  m_payload.set_archived(archived);
}

bool cta::objectstore::Tape::isDisabled() {
  checkPayloadReadable();
  return m_payload.disabled();
}

void cta::objectstore::Tape::setDisabled(bool disabled) {
  checkPayloadWritable();
  m_payload.set_disabled(disabled);
}

bool cta::objectstore::Tape::isReadOnly() {
  checkPayloadReadable();
  return m_payload.readonly();
}

void cta::objectstore::Tape::setReadOnly(bool readOnly) {
  checkPayloadWritable();
  m_payload.set_readonly(readOnly);
}

bool cta::objectstore::Tape::isFull() {
  checkPayloadReadable();
  return m_payload.full();
}

void cta::objectstore::Tape::setFull(bool full) {
  checkPayloadWritable();
  m_payload.set_full(full);
}

bool cta::objectstore::Tape::isBusy() {
  checkPayloadReadable();
  return m_payload.busy();
}

void cta::objectstore::Tape::setBusy(const std::string& drive, MountType mountType, 
  const std::string & host, time_t startTime, const std::string& agentAddress) {
  checkPayloadWritable();
  m_payload.set_busy(true);
  auto * cm = m_payload.mutable_currentmount();
  cm->set_drivemodel("");
  cm->set_driveserial("");
  cm->set_drivename(drive);
  cm->set_host(host);
  cm->set_time((uint64_t)time);
  switch (mountType) {
    case MountType::Archive:
      m_payload.set_currentmounttype(objectstore::serializers::MountType::Archive);
      break;
    case MountType::Retrieve:
      m_payload.set_currentmounttype(objectstore::serializers::MountType::Retrieve);
      break;
  }
}

void cta::objectstore::Tape::releaseBusy() {
  checkPayloadWritable();
  m_payload.set_busy(false);
}

void cta::objectstore::Tape::removeJob(const std::string& retriveToFileAddress) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_retrievejobs();
  bool found=false;
  do {
    found=false;
    found = false;
    // Push the found entry all the way to the end.
    for (size_t i=0; i<(size_t)jl->size(); i++) {
      if (jl->Get(i).address() == retriveToFileAddress) {
        found = true;
        while (i+1 < (size_t)jl->size()) {
          jl->SwapElements(i, i+1);
          i++;
        }
        break;
      }
    }
    // and remove it
    if (found)
      jl->RemoveLast();
  } while (found);
}










