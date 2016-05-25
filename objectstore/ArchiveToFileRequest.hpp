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

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "common/archiveNS/ArchiveFile.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLog;

class ArchiveToFileRequest: public ObjectOps<serializers::ArchiveToFileRequest, serializers::ArchiveToFileRequest_t> {
public:
  ArchiveToFileRequest(const std::string & address, Backend & os);
  ArchiveToFileRequest(Backend & os);
  ArchiveToFileRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(uint16_t copyNumber, const std::string & tapepool,
    const std::string & archivequeueaddress);
  void setJobFailureLimits(uint16_t copyNumber,
    uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries);
  void setJobSelected(uint16_t copyNumber, const std::string & owner);
  void setJobPending(uint16_t copyNumber);
  bool setJobSuccessful(uint16_t copyNumber); //< returns true if this is the last job
  bool addJobFailure(uint16_t copyNumber, uint64_t sessionId); //< returns true the job failed
  serializers::ArchiveJobStatus getJobStatus(uint16_t copyNumber);
  // Handling of the consequences of a job status change for the entire request.
  // This function returns true if the request got finished.
  bool finishIfNecessary();
  // Mark all jobs as pending mount (following their linking to a archive queue)
  void setAllJobsLinkingToArchiveQueue();
  // Mark all the jobs as being deleted, in case of a cancellation
  void setAllJobsFailed();
  // Mark all the jobs as pending deletion from NS.
  void setAllJobsPendingNSdeletion();
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // Set a job ownership
  void setJobOwner(uint16_t copyNumber, const std::string & owner);
  // Request management ========================================================
  void setSuccessful();
  void setFailed();
  // ===========================================================================
  void setArchiveFile(const cta::common::archiveNS::ArchiveFile & archiveFile);
  cta::common::archiveNS::ArchiveFile getArchiveFile();
  void setRemoteFile (const RemotePathAndStatus & remoteFile);
  cta::RemotePathAndStatus getRemoteFile();
  void setPriority (uint64_t priority);
  uint64_t getPriority();
  void setEntryLog (const objectstore::EntryLog& creationLog);
  EntryLog getCreationLog();
  void setArchiveToDirRequestAddress(const std::string & dirRequestAddress);
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tapePool;
    std::string ArchiveQueueAddress;
  };
  std::list<JobDump> dumpJobs();
  void garbageCollect(const std::string &presumedOwner) {}
  std::string  dump();
};

}}
