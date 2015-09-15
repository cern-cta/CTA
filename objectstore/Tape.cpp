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
    const std::string &logicallibrary) {
  ObjectOps<serializers::Tape>::initialize();
  // Set the reguired fields
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










