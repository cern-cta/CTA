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
#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/ArchiveFile.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLog;

class RetrieveRequest: public ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t> {
public:
  RetrieveRequest(const std::string & address, Backend & os);
  RetrieveRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(const cta::common::dataStructures::TapeFile & tapeFile,
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
  void setArchiveFile(const cta::common::dataStructures::ArchiveFile & archiveFile);
  cta::common::dataStructures::ArchiveFile getArchiveFile();

  void setDiskpoolName(const std::string &diskpoolName);
  std::string getDiskpoolName();

  void setDiskpoolThroughput(const uint64_t diskpoolThroughput);
  uint64_t getDiskpoolThroughput();

  void setDrData(const cta::common::dataStructures::DRData &drData);
  cta::common::dataStructures::DRData getDrData();

  void setDstURL(const std::string &dstURL);
  std::string getDstURL();

  void setRequester(const cta::common::dataStructures::UserIdentity &requester);
  cta::common::dataStructures::UserIdentity getRequester();

  void setEntryLog (const objectstore::EntryLog& entryLog);
  objectstore::EntryLog getEntryLog();
  // ===========================================================================
  std::list<JobDump> dumpJobs();
  std::string dump();
};

}}
