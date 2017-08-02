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

#include "ArchiveRequest.hpp"
#include "GenericObject.hpp"
#include "ArchiveQueue.hpp"
#include "Helpers.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "MountPolicySerDeser.hpp"

#include <algorithm>
#include <google/protobuf/util/json_util.h>

namespace cta { namespace objectstore {

cta::objectstore::ArchiveRequest::ArchiveRequest(const std::string& address, Backend& os): 
  ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t>(os, address){ }

cta::objectstore::ArchiveRequest::ArchiveRequest(Backend& os): 
  ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t>(os) { }

cta::objectstore::ArchiveRequest::ArchiveRequest(GenericObject& go):
  ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::ArchiveRequest::initialize() {
  // Setup underlying object
  ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t>::initialize();
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void cta::objectstore::ArchiveRequest::addJob(uint16_t copyNumber,
  const std::string& tapepool, const std::string& archivequeueaddress, 
    uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries) {
  checkPayloadWritable();
  auto *j = m_payload.add_jobs();
  j->set_copynb(copyNumber);
  j->set_status(serializers::ArchiveJobStatus::AJS_LinkingToArchiveQueue);
  j->set_tapepool(tapepool);
  j->set_owner(archivequeueaddress);
  // XXX This field (archivequeueaddress) is a leftover from a past layout when tape pools were static
  // in the object store, and should be eventually removed.
  j->set_archivequeueaddress("");
  j->set_totalretries(0);
  j->set_retrieswithinmount(0);
  j->set_lastmountwithfailure(0);
  j->set_maxretrieswithinmount(maxRetiesWithinMount);
  j->set_maxtotalretries(maxTotalRetries);
}

bool cta::objectstore::ArchiveRequest::setJobSuccessful(uint16_t copyNumber) {
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
  throw NoSuchJob("In ArchiveRequest::setJobSuccessful(): job not found");
}

bool cta::objectstore::ArchiveRequest::addJobFailure(uint16_t copyNumber,
    uint64_t mountId) {
  checkPayloadWritable();
  auto * jl = m_payload.mutable_jobs();
  // Find the job and update the number of failures 
  // (and return the job status: failed (true) or to be retried (false))
  for (auto & j: *jl) {
    if (j.copynb() == copyNumber) {
      if (j.lastmountwithfailure() == mountId) {
        j.set_retrieswithinmount(j.retrieswithinmount() + 1);
      } else {
        j.set_retrieswithinmount(1);
        j.set_lastmountwithfailure(mountId);
      }
      j.set_totalretries(j.totalretries() + 1);
    }
    if (j.totalretries() >= j.maxtotalretries()) {
      j.set_status(serializers::AJS_Failed);
      finishIfNecessary();
      return true;
    } else {
      j.set_status(serializers::AJS_PendingMount);
      return false;
    }
  }
  throw NoSuchJob ("In ArchiveRequest::addJobFailure(): could not find job");
}


void cta::objectstore::ArchiveRequest::setAllJobsLinkingToArchiveQueue() {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    j->set_status(serializers::AJS_LinkingToArchiveQueue);
  }
}

void cta::objectstore::ArchiveRequest::setAllJobsFailed() {
  checkPayloadWritable();
  auto * jl=m_payload.mutable_jobs();
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    j->set_status(serializers::AJS_Failed);
  }
}

void ArchiveRequest::setArchiveFile(const cta::common::dataStructures::ArchiveFile& archiveFile) {
  checkPayloadWritable();
  // TODO: factor out the archivefile structure from the flat ArchiveRequest.
  m_payload.set_archivefileid(archiveFile.archiveFileID);
  m_payload.set_checksumtype(archiveFile.checksumType);
  m_payload.set_checksumvalue(archiveFile.checksumValue);
  m_payload.set_creationtime(archiveFile.creationTime);
  m_payload.set_diskfileid(archiveFile.diskFileId);
  m_payload.mutable_diskfileinfo()->set_group(archiveFile.diskFileInfo.group);
  m_payload.mutable_diskfileinfo()->set_owner(archiveFile.diskFileInfo.owner);
  m_payload.mutable_diskfileinfo()->set_path(archiveFile.diskFileInfo.path);
  m_payload.mutable_diskfileinfo()->set_recoveryblob(archiveFile.diskFileInfo.recoveryBlob);
  m_payload.set_diskinstance(archiveFile.diskInstance);
  m_payload.set_filesize(archiveFile.fileSize);
  m_payload.set_reconcilationtime(archiveFile.reconciliationTime);
  m_payload.set_storageclass(archiveFile.storageClass);
}

cta::common::dataStructures::ArchiveFile ArchiveRequest::getArchiveFile() {
  checkPayloadReadable();
  cta::common::dataStructures::ArchiveFile ret;
  ret.archiveFileID = m_payload.archivefileid();
  ret.checksumType = m_payload.checksumtype();
  ret.checksumValue = m_payload.checksumvalue();
  ret.creationTime = m_payload.creationtime();
  ret.diskFileId = m_payload.diskfileid();
  ret.diskFileInfo.group = m_payload.diskfileinfo().group();
  ret.diskFileInfo.owner = m_payload.diskfileinfo().owner();
  ret.diskFileInfo.path = m_payload.diskfileinfo().path();
  ret.diskFileInfo.recoveryBlob = m_payload.diskfileinfo().recoveryblob();
  ret.diskInstance = m_payload.diskinstance();
  ret.fileSize = m_payload.filesize();
  ret.reconciliationTime = m_payload.reconcilationtime();
  ret.storageClass = m_payload.storageclass();
  return ret;
}

//------------------------------------------------------------------------------
// setArchiveReportURL
//------------------------------------------------------------------------------
void ArchiveRequest::setArchiveReportURL(const std::string& URL) {
  checkPayloadWritable();
  m_payload.set_archivereporturl(URL);
}

//------------------------------------------------------------------------------
// getArviveReportURL
//------------------------------------------------------------------------------
std::string ArchiveRequest::getArchiveReportURL() {
  checkPayloadReadable();
  return m_payload.archivereporturl();
}


//------------------------------------------------------------------------------
// setMountPolicy
//------------------------------------------------------------------------------
void cta::objectstore::ArchiveRequest::setMountPolicy(const cta::common::dataStructures::MountPolicy &mountPolicy) {
  checkPayloadWritable();
  MountPolicySerDeser(mountPolicy).serialize(*m_payload.mutable_mountpolicy());
}

//------------------------------------------------------------------------------
// getMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::objectstore::ArchiveRequest::getMountPolicy() {
  checkPayloadReadable();
  MountPolicySerDeser mp;
  mp.deserialize(m_payload.mountpolicy());
  return mp;
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void ArchiveRequest::setRequester(const cta::common::dataStructures::UserIdentity &requester) {
  checkPayloadWritable();
  auto payloadRequester = m_payload.mutable_requester();
  payloadRequester->set_name(requester.name);
  payloadRequester->set_group(requester.group);
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity ArchiveRequest::getRequester() {
  checkPayloadReadable();
  cta::common::dataStructures::UserIdentity requester;
  auto payloadRequester = m_payload.requester();
  requester.name=payloadRequester.name();
  requester.group=payloadRequester.group();
  return requester;
}

//------------------------------------------------------------------------------
// setSrcURL
//------------------------------------------------------------------------------
void ArchiveRequest::setSrcURL(const std::string &srcURL) {
  checkPayloadWritable();
  m_payload.set_srcurl(srcURL);
}

//------------------------------------------------------------------------------
// getSrcURL
//------------------------------------------------------------------------------
std::string ArchiveRequest::getSrcURL() {
  checkPayloadReadable();
  return m_payload.srcurl();
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void ArchiveRequest::setEntryLog(const cta::common::dataStructures::EntryLog &creationLog) {
  checkPayloadWritable();
  auto payloadCreationLog = m_payload.mutable_creationlog();
  payloadCreationLog->set_time(creationLog.time);
  payloadCreationLog->set_host(creationLog.host);
  payloadCreationLog->set_username(creationLog.username);
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog ArchiveRequest::getEntryLog() {
  checkPayloadReadable();
  EntryLogSerDeser el;
  el.deserialize(m_payload.creationlog());
  return el;
}

auto ArchiveRequest::dumpJobs() -> std::list<ArchiveRequest::JobDump> {
  checkPayloadReadable();
  std::list<JobDump> ret;
  auto & jl = m_payload.jobs();
  for (auto j=jl.begin(); j!=jl.end(); j++) {
    ret.push_back(JobDump());
    ret.back().copyNb = j->copynb();
    ret.back().tapePool = j->tapepool();
    ret.back().owner = j->owner();
  }
  return ret;
}

//------------------------------------------------------------------------------
// garbageCollect
//------------------------------------------------------------------------------
void ArchiveRequest::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  // The behavior here depends on which job the agent is supposed to own.
  // We should first find this job (if any). This is for covering the case
  // of a selected job. The Request could also still being connected to tape
  // pools. In this case we will finish the connection to tape pools unconditionally.
  auto * jl = m_payload.mutable_jobs();
  bool anythingGarbageCollected=false;
  for (auto j=jl->begin(); j!=jl->end(); j++) {
    auto owner=j->owner();
    auto status=j->status();
    if (status==serializers::AJS_LinkingToArchiveQueue ||
        ( (status==serializers::AJS_Selected || status==serializers::AJS_PendingMount)
             && owner==presumedOwner)) {
        // If the job was being connected to the tape pool or was selected
        // by the dead agent, then we have to ensure it is indeed connected to
        // the tape pool and set its status to pending.
        // (Re)connect the job to the tape pool and make it pending.
        // If we fail to reconnect, we have to fail the job and potentially
        // finish the request.
      std::string queueObject="Not defined yet";
      anythingGarbageCollected=true;
      try {
        // Get the queue where we should requeue the job. The queue might need to be
        // recreated (this will be done by helper).
        ArchiveQueue aq(m_objectStore);
        ScopedExclusiveLock tpl;
        Helpers::getLockedAndFetchedQueue<ArchiveQueue>(aq, tpl, agentReference, j->tapepool(), lc);
        queueObject=aq.getAddressIfSet();
        ArchiveRequest::JobDump jd;
        jd.copyNb = j->copynb();
        jd.tapePool = j->tapepool();
        jd.owner = j->owner();
        if (aq.addJobIfNecessary(jd, getAddressIfSet(), getArchiveFile().archiveFileID,
          getArchiveFile().fileSize, getMountPolicy(), getEntryLog().time))
          aq.commit();
        j->set_owner(aq.getAddressIfSet());
        j->set_status(serializers::AJS_PendingMount);
        commit();
        log::ScopedParamContainer params(lc);
        params.add("jobObject", getAddressIfSet())
              .add("queueObject", queueObject)
              .add("presumedOwner", presumedOwner)
              .add("copyNb", j->copynb());
        lc.log(log::INFO, "In ArchiveRequest::garbageCollect(): requeued job.");
      } catch (...) {
        // We could not requeue the job: fail it.
        j->set_status(serializers::AJS_Failed);
        log::ScopedParamContainer params(lc);
        params.add("jobObject", getAddressIfSet())
              .add("queueObject", queueObject)
              .add("presumedOwner", presumedOwner)
              .add("copyNb", j->copynb());
        // Log differently depending on the exception type.
        try {
          std::rethrow_exception(std::current_exception());
        } catch (cta::exception::Exception &ex) {
          params.add("exceptionMessage", ex.getMessageValue());
        } catch (std::exception & ex) {
          params.add("exceptionWhat", ex.what());
        } catch (...) {
          params.add("exceptionType", "unknown");
        }        
        // This could be the end of the request, with various consequences.
        // This is handled here:
        if (finishIfNecessary()) {
          lc.log(log::ERR, "In ArchiveRequest::garbageCollect(): failed to requeue the job. Failed it and removed the request as a consequence. Backtrace follows.");
          lc.logBacktrace(log::ERR, "In ArchiveRequest::garbageCollect(): ");
          return;
        } else {
          commit();
          lc.log(log::ERR, "In ArchiveRequest::garbageCollect(): failed to requeue the job and failed it.");
        }
      }
    }
  }
  if (!anythingGarbageCollected) {
    log::ScopedParamContainer params(lc);
    params.add("jobObject", getAddressIfSet())
          .add("presumedOwner", presumedOwner);
    lc.log(log::INFO, "In ArchiveRequest::garbageCollect(): nothing to garbage collect.");
  }
}

void ArchiveRequest::setJobOwner(
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
  throw NoSuchJob("In ArchiveRequest::setJobOwner: no such job");
}

ArchiveRequest::AsyncJobOwnerUpdater* ArchiveRequest::asyncUpdateJobOwner(uint16_t copyNumber,
  const std::string& owner, const std::string& previousOwner) {
  std::unique_ptr<AsyncJobOwnerUpdater> ret(new AsyncJobOwnerUpdater);
  // Passing a reference to the unique pointer led to strange behaviors.
  auto & retRef = *ret;
  ret->m_updaterCallback=
      [this, copyNumber, owner, previousOwner, &retRef](const std::string &in)->std::string {
        // We have a locked and fetched object, so we just need to work on its representation.
        serializers::ObjectHeader oh;
        if (!oh.ParseFromString(in)) {
          // Use a the tolerant parser to assess the situation.
          oh.ParsePartialFromString(in);
          throw cta::exception::Exception(std::string("In ArchiveRequest::asyncUpdateJobOwner(): could not parse header: ")+
            oh.InitializationErrorString());
        }
        if (oh.type() != serializers::ObjectType::ArchiveRequest_t) {
          std::stringstream err;
          err << "In ArchiveRequest::asyncUpdateJobOwner()::lambda(): wrong object type: " << oh.type();
          throw cta::exception::Exception(err.str());
        }
        serializers::ArchiveRequest payload;
        if (!payload.ParseFromString(oh.payload())) {
          // Use a the tolerant parser to assess the situation.
          payload.ParsePartialFromString(oh.payload());
          throw cta::exception::Exception(std::string("In ArchiveRequest::asyncUpdateJobOwner(): could not parse payload: ")+
            payload.InitializationErrorString());
        }
        // Find the copy number and change the owner.
        auto *jl=payload.mutable_jobs();
        for (auto j=jl->begin(); j!=jl->end(); j++) {
          if (j->copynb() == copyNumber) {
            if (j->owner() != previousOwner) {
              throw WrongPreviousOwner("In ArchiveRequest::asyncUpdateJobOwner()::lambda(): Job not owned.");
            }
            j->set_owner(owner);
            // We also need to gather all the job content for the user to get in-memory
            // representation.
            // TODO this is an unfortunate duplication of the getXXX() members of ArchiveRequest.
            // We could try and refactor this.
            retRef.m_archiveFile.archiveFileID = payload.archivefileid();
            retRef.m_archiveFile.checksumType = payload.checksumtype();
            retRef.m_archiveFile.checksumValue = payload.checksumvalue();
            retRef.m_archiveFile.creationTime = payload.creationtime();
            retRef.m_archiveFile.diskFileId = payload.diskfileid();
            retRef.m_archiveFile.diskFileInfo.group = payload.diskfileinfo().group();
            retRef.m_archiveFile.diskFileInfo.owner = payload.diskfileinfo().owner();
            retRef.m_archiveFile.diskFileInfo.path = payload.diskfileinfo().path();
            retRef.m_archiveFile.diskFileInfo.recoveryBlob = payload.diskfileinfo().recoveryblob();
            retRef.m_archiveFile.diskInstance = payload.diskinstance();
            retRef.m_archiveFile.fileSize = payload.filesize();
            retRef.m_archiveFile.reconciliationTime = payload.reconcilationtime();
            retRef.m_archiveFile.storageClass = payload.storageclass();
            retRef.m_archiveReportURL = payload.archivereporturl();
            retRef.m_srcURL = payload.srcurl();
            oh.set_payload(payload.SerializePartialAsString());
            return oh.SerializeAsString();
          }
        }
        // If we do not find the copy, return not owned as well...
        throw WrongPreviousOwner("In ArchiveRequest::asyncUpdateJobOwner()::lambda(): copyNb not found.");
      };
  ret->m_backendUpdater.reset(m_objectStore.asyncUpdate(getAddressIfSet(), ret->m_updaterCallback));
  return ret.release();
}

void ArchiveRequest::AsyncJobOwnerUpdater::wait() {
  m_backendUpdater->wait();
}

const common::dataStructures::ArchiveFile& ArchiveRequest::AsyncJobOwnerUpdater::getArchiveFile() {
  return m_archiveFile;
}

const std::string& ArchiveRequest::AsyncJobOwnerUpdater::getArchiveReportURL() {
  return m_archiveReportURL;
}

const std::string& ArchiveRequest::AsyncJobOwnerUpdater::getSrcURL() {
  return m_srcURL;
}

std::string ArchiveRequest::getJobOwner(uint16_t copyNumber) {
  checkPayloadReadable();
  auto jl = m_payload.jobs();
  auto j=std::find_if(jl.begin(), jl.end(), [&](decltype(*jl.begin())& j2){ return j2.copynb() == copyNumber; });
  if (jl.end() == j)
    throw NoSuchJob("In ArchiveRequest::getJobOwner: no such job");
  return j->owner();
}


bool ArchiveRequest::finishIfNecessary() {
  checkPayloadWritable();
  // This function is typically called after changing the status of one job
  // in memory. If the request is complete, we will just remove it.
  // If all the jobs are either complete or failed, we can remove the request.
  auto & jl=m_payload.jobs();
  using serializers::ArchiveJobStatus;
  std::set<serializers::ArchiveJobStatus> finishedStatuses(
    {ArchiveJobStatus::AJS_Complete, ArchiveJobStatus::AJS_Failed});
  for (auto & j: jl)
    if (!finishedStatuses.count(j.status()))
      return false;
  remove();
  return true;
}

std::string ArchiveRequest::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

}} // namespace cta::objectstore


