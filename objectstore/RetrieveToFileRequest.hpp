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
#include <list>
#include "common/archiveNS/ArchiveFile.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class CreationLog;

class RetrieveToFileRequest: public ObjectOps<serializers::RetrieveToFileRequest> {
public:
  RetrieveToFileRequest(const std::string & address, Backend & os);
  RetrieveToFileRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(const cta::TapeFileLocation & tapeFileLocation,
    const std::string & tapeaddress);
  void setJobFailureLimits(uint16_t copyNumber,
    uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries);
  void setJobSelected(uint16_t copyNumber, const std::string & owner);
  void setJobPending(uint16_t copyNumber);
  bool setJobSuccessful(uint16_t copyNumber); //< returns true if this is the last job
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tape;
    std::string tapeAddress;
    uint64_t fseq;
    uint64_t blockid;
  };
  JobDump getJob(uint16_t copyNb);
  struct FailuresCount {
    uint16_t failuresWithinMount;
    uint16_t totalFailures;
  };
  FailuresCount addJobFailure(uint16_t copyNumber, uint64_t sessionId);
  serializers::RetrieveJobStatus getJobStatus(uint16_t copyNumber);
  // Handling of the consequences of a job status. This is simpler that archival
  // as one finish is enough.
  void finish();
  // Mark all jobs as pending mount (following their linking to a tape pool)
  void setAllJobsLinkingToTapePool();
  // Mark all the jobs as being deleted, in case of a cancellation
  void setAllJobsFailed();
  // Mark all the jobs as pending deletion from NS.
  void setAllJobsPendingNSdeletion();
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // Request management ========================================================
  void setSuccessful();
  void setFailed();
  // ===========================================================================
  void setArchiveFile(const cta::common::archiveNS::ArchiveFile & archiveFile);
  cta::common::archiveNS::ArchiveFile getArchiveFile();
  void setRemoteFile (const std::string & remoteFile);
  std::string getRemoteFile();
  void setPriority (uint64_t priority);
  uint64_t getPriority();
  void setCreationLog (const objectstore::CreationLog& creationLog);
  CreationLog getCreationLog();
  void setRetrieveToDirRequestAddress(const std::string & dirRequestAddress);
  void setSize(uint64_t size);
  uint64_t getSize();
  std::list<JobDump> dumpJobs();
  std::string dump();
};

}}
