
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

#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/Requester.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class CreationLog;

class ArchiveRequest: public ObjectOps<serializers::ArchiveRequest> {
public:
  ArchiveRequest(const std::string & address, Backend & os);
  ArchiveRequest(Backend & os);
  ArchiveRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(uint16_t copyNumber, const std::string & tapepool,
    const std::string & tapepooladdress);
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
  // Mark all jobs as pending mount (following their linking to a tape pool)
  void setAllJobsLinkingToTapePool();
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
  void setChecksumType(const std::string &checksumType);
  std::string getChecksumType();

  void setChecksumValue(const std::string &checksumValue);
  std::string getChecksumValue();

  void setDiskpoolName(const std::string &diskpoolName);
  std::string getDiskpoolName();

  void setDiskpoolThroughput(const uint64_t diskpoolThroughput);
  uint64_t getDiskpoolThroughput();

  void setDrData(const cta::common::dataStructures::DRData &drData);
  cta::common::dataStructures::DRData getDrData();

  void setEosFileID(const std::string &eosFileID);
  std::string getEosFileID();

  void setFileSize(const uint64_t fileSize);
  uint64_t getFileSize();

  void setRequester(const cta::common::dataStructures::Requester &requester);
  cta::common::dataStructures::Requester getRequester();

  void setSrcURL(const std::string &srcURL);
  std::string getSrcURL();

  void setStorageClass(const std::string &storageClass);
  std::string getStorageClass();

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog();
  
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tapePool;
    std::string tapePoolAddress;
  };
  
  std::list<JobDump> dumpJobs();
  void garbageCollect(const std::string &presumedOwner);
  std::string  dump();
};

}}
