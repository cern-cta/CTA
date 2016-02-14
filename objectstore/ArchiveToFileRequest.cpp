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
#include "UserIdentity.hpp"
#include <json-c/json.h>

cta::objectstore::ArchiveToFileRequest::ArchiveToFileRequest(
  const std::string& address, Backend& os): 
  ObjectOps<serializers::ArchiveToFileRequest>(os, address) { }

cta::objectstore::ArchiveToFileRequest::ArchiveToFileRequest(
  Backend& os): 
  ObjectOps<serializers::ArchiveToFileRequest>(os) { }

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
  j->set_lastmountwithfailure(0);
  // Those 2 values are set to 0 as at creation time, we do not read the 
  // tape pools yet.
  j->set_maxretrieswithinmount(0);
  j->set_maxtotalretries(0);
}

bool cta::objectstore::ArchiveToFileRequest::setJobSuccessful(uint16_t copyNumber) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    if (j->copynb() == copyNumber) {
      j->set_status(serializers::ArchiveJobStatus::AJS_Complete);
      for (auto j2=jl->begin(); j2!=jl->end(); j2++) {
        if (j2->status()!= serializers::ArchiveJobStatus::AJS_Complete && 
            j2->status()!= serializers::ArchiveJobStatus::AJS_Failed)
          return false;
      }
      return true;
    }
  }
  throw NoSuchJob("In ArchiveToFileRequest::setJobSuccessful(): job not found");
}


void cta::objectstore::ArchiveToFileRequest::setJobFailureLimits(uint16_t copyNumber,
    uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    if (j->copynb() == copyNumber) {
      j->set_maxretrieswithinmount(maxRetiesWithinMount);
      j->set_maxtotalretries(maxTotalRetries);
      return;
    }
  }
  throw NoSuchJob("In ArchiveToFileRequest::setJobFailureLimits(): job not found");
}

bool cta::objectstore::ArchiveToFileRequest::addJobFailure(uint16_t copyNumber,
    uint64_t mountId) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  // Find the job and update the number of failures (and return the new count)
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    if (j->copynb() == copyNumber) {
      if (j->lastmountwithfailure() == mountId) {
        j->set_retrieswithinmount(j->retrieswithinmount() + 1);
      } else {
        j->set_retrieswithinmount(1);
        j->set_lastmountwithfailure(mountId);
      }
      j->set_totalretries(j->totalretries() + 1);
    }
    if (j->totalretries() >= j->maxtotalretries()) {
      j->set_status(serializers::AJS_Failed);
      finishIfNecessary();
      return true;
    } else {
      j->set_status(serializers::AJS_PendingMount);
      return false;
    }
  }
  throw NoSuchJob ("In ArchiveToFileRequest::addJobFailure(): could not find job");
}


void cta::objectstore::ArchiveToFileRequest::setAllJobsLinkingToTapePool() {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    j->set_status(serializers::AJS_LinkingToTapePool);
  }
}

void cta::objectstore::ArchiveToFileRequest::setAllJobsFailed() {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    j->set_status(serializers::AJS_Failed);
  }
}

void cta::objectstore::ArchiveToFileRequest::setAllJobsPendingNSdeletion() {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    j->set_status(serializers::AJS_PendingNsDeletion);
  }
}

void cta::objectstore::ArchiveToFileRequest::setArchiveFile(
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

cta::common::archiveNS::ArchiveFile cta::objectstore::ArchiveToFileRequest::getArchiveFile() {
  checkPayloadReadable();
  auto checksum = m_payload.archivefile().checksum();
  auto fileId = m_payload.archivefile().fileid();
  const time_t lastModificationTime = m_payload.archivefile().lastmodificationtime();
  auto nsHostName = m_payload.archivefile().nshostname();
  auto path = m_payload.archivefile().path();
  auto size = m_payload.archivefile().size();
  return common::archiveNS::ArchiveFile{path, nsHostName, fileId, size, checksum, lastModificationTime};
}


void cta::objectstore::ArchiveToFileRequest::setRemoteFile(
  const RemotePathAndStatus& remoteFile) {
  checkPayloadWritable();
  m_payload.mutable_remotefile()->set_mode(remoteFile.status.mode);
  m_payload.mutable_remotefile()->set_size(remoteFile.status.size);
  m_payload.mutable_remotefile()->set_path(remoteFile.path.getRaw());
  cta::objectstore::UserIdentity ui(remoteFile.status.owner.uid, remoteFile.status.owner.gid);
  ui.serialize(*m_payload.mutable_remotefile()->mutable_owner());
}

cta::RemotePathAndStatus cta::objectstore::ArchiveToFileRequest::getRemoteFile() {
  checkPayloadReadable();
  RemotePath retPath(m_payload.remotefile().path());
  cta::objectstore::UserIdentity ui;
  ui.deserialize(m_payload.remotefile().owner());
  RemoteFileStatus retStatus(ui,
      m_payload.remotefile().mode(),
      m_payload.remotefile().size());
  return cta::RemotePathAndStatus(retPath, retStatus);
}


void cta::objectstore::ArchiveToFileRequest::setPriority(uint64_t priority) {
  checkPayloadWritable();
  m_payload.set_priority(priority);
}

uint64_t cta::objectstore::ArchiveToFileRequest::getPriority() {
  checkPayloadReadable();
  return m_payload.priority();
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

void cta::objectstore::ArchiveToFileRequest::garbageCollect(const std::string &presumedOwner) {
  checkPayloadWritable();
  // The behavior here depends on which job the agent is supposed to own.
  // We should first find this job (if any). This is for covering the case
  // of a selected job. The Request could also still being connected to tape
  // pools. In this case we will finish the connection to tape pools unconditionally.
  auto * jl = m_payload.mutable_jobs();
  auto s= m_payload.remotefile().size();
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
          m_payload.archivefile().path(), m_payload.remotefile().size()))
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
          m_payload.archivefile().path(), m_payload.remotefile().size()))
          tp.commit();
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
          m_payload.archivefile().path(), m_payload.remotefile().size()))
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

void cta::objectstore::ArchiveToFileRequest::setJobOwner(
  uint16_t copyNumber, const std::string& owner) {
  checkPayloadWritable();
  // Find the right job
  auto mutJobs = m_payload.mutable_jobs();
  for (auto job=mutJobs->begin(); job!=mutJobs->end(); job++) {
    if (job->copynb() == copyNumber) {
      job->set_owner(owner);
      return;
    }
  }
  throw NoSuchJob("In ArchiveToFileRequest::setJobOwner: no such job");
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

std::string cta::objectstore::ArchiveToFileRequest::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "ArchiveToFileRequest" << std::endl;
  struct json_object * jo = json_object_new_object();
  json_object_object_add(jo, "archivetodiraddress", json_object_new_string(m_payload.archivetodiraddress().c_str()));
  json_object_object_add(jo, "priority", json_object_new_int64(m_payload.priority()));
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
    json_object_object_add(jj, "copynb", json_object_new_int64(j->copynb()));
    json_object_object_add(jj, "lastmountwithfailure", json_object_new_int64(j->lastmountwithfailure()));
    json_object_object_add(jj, "maxretrieswithinmount", json_object_new_int64(j->maxretrieswithinmount()));
    json_object_object_add(jj, "maxtotalretries", json_object_new_int64(j->maxtotalretries()));
    json_object_object_add(jj, "owner", json_object_new_string(j->owner().c_str()));
    json_object_object_add(jj, "retrieswithinmount", json_object_new_int64(j->retrieswithinmount()));
    json_object_object_add(jj, "status", json_object_new_int64(j->status()));
    json_object_object_add(jj, "tapepool", json_object_new_string(j->tapepool().c_str()));
    json_object_object_add(jj, "tapepoolAddress", json_object_new_string(j->tapepooladdress().c_str()));
    json_object_object_add(jj, "totalRetries", json_object_new_int64(j->totalretries()));
    json_object_array_add(jja, jj);
  }
  json_object_object_add(jo, "jobs", jja);
  // Object for log
  json_object * jlog = json_object_new_object();
  json_object_object_add(jlog, "comment", json_object_new_string(m_payload.log().comment().c_str()));
  json_object_object_add(jlog, "host", json_object_new_string(m_payload.log().host().c_str()));
  json_object_object_add(jlog, "time", json_object_new_int64(m_payload.log().time()));
  json_object_object_add(jo, "log", jlog);
  // Object for remote file
  json_object * jrf = json_object_new_object();
  json_object_object_add(jrf, "mode", json_object_new_int(m_payload.remotefile().mode()));
  json_object_object_add(jrf, "path", json_object_new_string(m_payload.remotefile().path().c_str()));
  json_object_object_add(jrf, "size", json_object_new_int64(m_payload.remotefile().size()));
  json_object_object_add(jo, "remoteFile", jrf);
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  json_object_put(jo);
  return ret.str();
}




