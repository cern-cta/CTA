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
#include "TapePool.hpp"

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
  j->set_status(serializers::ArchiveJobStatus::AJS_PendingNsCreation);
  j->set_tapepool(tapepool);
  j->set_owner("");
  j->set_tapepooladdress(tapepooladdress);
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
}

void cta::objectstore::ArchiveToFileRequest::setArchiveFile(
  const std::string& archiveFile) {
  checkPayloadWritable();
  m_payload.set_archivefile(archiveFile);
}

std::string cta::objectstore::ArchiveToFileRequest::getArchiveFile() {
  checkPayloadReadable();
  return m_payload.archivefile();
}


void cta::objectstore::ArchiveToFileRequest::setRemoteFile(
  const std::string& remoteFile) {
  checkPayloadWritable();
  m_payload.set_remotefile(remoteFile);
}

std::string cta::objectstore::ArchiveToFileRequest::getRemoteFile() {
  checkPayloadReadable();
  return m_payload.remotefile();
}


void cta::objectstore::ArchiveToFileRequest::setPriority(uint64_t priority) {
  checkPayloadWritable();
  m_payload.set_priority(priority);
}

void cta::objectstore::ArchiveToFileRequest::setCreationLog(
  const objectstore::CreationLog& creationLog) {
  checkPayloadWritable();
  creationLog.serialize(*m_payload.mutable_log());
}

auto cta::objectstore::ArchiveToFileRequest::getCreationLog() -> CreationLog {
  checkPayloadReadable();
  CreationLog ret;
  ret.deserialize(m_payload.log());
  return ret;
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
    ret.back().tapePoolAddress = j->tapepooladdress();
  }
  return ret;
}

uint64_t cta::objectstore::ArchiveToFileRequest::getSize() {
  checkPayloadReadable();
  return m_payload.size();
}

void cta::objectstore::ArchiveToFileRequest::setSize(uint64_t size) {
  checkPayloadWritable();
  m_payload.set_size(size);
}

void cta::objectstore::ArchiveToFileRequest::garbageCollect(const std::string &presumedOwner) {
  checkPayloadWritable();
  // The behavior here depends on which job the agent is supposed to own.
  // We should first find this job (if any). This is for covering the case
  // of a selected job. The Request could also still being connected to tape
  // pools. In this case we will finish the connection to tape pools unconditionally.
  auto * jl = m_payload.mutable_jobs();
  auto s= m_payload.size();
  s=s;
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    auto owner=j->owner();
    auto status=j->status();
    if (status==serializers::AJS_LinkingToTapePool ||
        (status==serializers::AJS_Selected && owner==presumedOwner)) {
        // If the job was being connected to the tape pool or was selected
        // by the dead agent, then we have to ensure it is indeed connected to
        // the tape pool and set its status to pending.
        // (Re)connect the job to the tape pool and make it pending.
        // If we fail to reconnect, we have to fail the job and potentially
        // finish the request.
      try {
        TapePool tp(j->tapepooladdress(), m_objectStore);
        ScopedExclusiveLock tpl(tp);
        tp.fetch();
        JobDump jd;
        jd.copyNb = j->copynb();
        jd.tapePool = j->tapepool();
        jd.tapePoolAddress = j->tapepooladdress();
        if (tp.addJobIfNecessary(jd, getAddressIfSet(), 
          m_payload.archivefile(), m_payload.size()))
          tp.commit();
        j->set_status(serializers::AJS_PendingMount);
        commit();
      } catch (...) {
        j->set_status(serializers::AJS_Failed);
        // This could be the end of the request, with various consequences.
        // This is handled here:
        if (finishIfNecessary())
          return;
      }
    } else if (status==serializers::AJS_PendingNsCreation) {
      // If the job is pending NsCreation, we have to queue it in the tape pool's
      // queue for files orphaned pending ns creation. Some user process will have
      // to pick them up actively (recovery involves schedulerDB + NameServerDB)
      try {
        TapePool tp(j->tapepooladdress(), m_objectStore);
        ScopedExclusiveLock tpl(tp);
        tp.fetch();
        JobDump jd;
        jd.copyNb = j->copynb();
        jd.tapePool = j->tapepool();
        jd.tapePoolAddress = j->tapepooladdress();
        if (tp.addOrphanedJobPendingNsCreation(jd, getAddressIfSet(), 
          m_payload.archivefile(), m_payload.size()))
          tp.commit();
        j->set_status(serializers::AJS_PendingMount);
        commit();
      } catch (...) {
        j->set_status(serializers::AJS_Failed);
        // This could be the end of the request, with various consequences.
        // This is handled here:
        if (finishIfNecessary())
          return;
      }
    } else if (status==serializers::AJS_PendingNsDeletion) {
      // If the job is pending NsDeletion, we have to queue it in the tape pool's
      // queue for files orphaned pending ns deletion. Some user process will have
      // to pick them up actively (recovery involves schedulerDB + NameServerDB)
      try {
        TapePool tp(j->tapepooladdress(), m_objectStore);
        ScopedExclusiveLock tpl(tp);
        tp.fetch();
        JobDump jd;
        jd.copyNb = j->copynb();
        jd.tapePool = j->tapepool();
        jd.tapePoolAddress = j->tapepooladdress();
        if (tp.addOrphanedJobPendingNsCreation(jd, getAddressIfSet(), 
          m_payload.archivefile(), m_payload.size()))
          tp.commit();
        j->set_status(serializers::AJS_PendingMount);
        commit();
      } catch (...) {
        j->set_status(serializers::AJS_Failed);
        // This could be the end of the request, with various consequences.
        // This is handled here:
        if (finishIfNecessary())
          return;
      }
    } else {
      return;
    }
  }
}

bool cta::objectstore::ArchiveToFileRequest::finishIfNecessary() {
  checkPayloadWritable();
  // This function is typically called after changing the status of one job
  // in memory. If the job is complete, we will just remove it.
  // TODO: we will have to push the result to the ArchiveToDirRequest when
  // it gets implemented.
  // If all the jobs are either complete or failed, we can remove the request.
  auto & jl=m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    if (j->status() != serializers::AJS_Complete 
        && j->status() != serializers::AJS_Failed) {
      return false;
    }
  }
  remove();
  return true;
}

      



