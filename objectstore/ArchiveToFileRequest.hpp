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
#include "common/RemotePathAndStatus.hpp"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class CreationLog;

class ArchiveToFileRequest: public ObjectOps<serializers::ArchiveToFileRequest> {
public:
  ArchiveToFileRequest(const std::string & address, Backend & os);
  ArchiveToFileRequest(Backend & os);
  ArchiveToFileRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(uint16_t copyNumber, const std::string & tapepool,
    const std::string & tapepooladdress);
  void jobSelect(uint16_t copyNumber, const std::string & owner);
  void jobSetPending(uint16_t copyNumber);
  void jobSetSuccessful(uint16_t copyNumber);
  void jobSetFailed(uint16_t copyNumber);
  // Duplication of the protbuff statuses
  enum jobStatus {
    AJS_LinkingToTapePool = 0,
    AJS_Pending = 1,
    AJS_Selected = 2,
    AJS_Complete = 3,
    AJS_Failed = 99
  };
  enum jobStatus getJobStatus(uint16_t copyNumber);
  // Handling of the consequences of a job status change for the entire request.
  // This function returns true if the request got finished.
  bool finishIfNecessary();
  // Mark all jobs as pending mount (following their linking to a tape pool)
  void setJobsLinkingToTapePool();
  // Request management ========================================================
  void setSuccessful();
  void setFailed();
  // ===========================================================================
  void setArchiveFile(const std::string & archiveFile);
  std::string getArchiveFile();
  void setRemoteFile (const RemotePathAndStatus & remoteFile);
  cta::RemotePathAndStatus getRemoteFile();
  void setPriority (uint64_t priority);
  uint64_t getPriority();
  void setCreationLog (const objectstore::CreationLog& creationLog);
  CreationLog getCreationLog();
  void setArchiveToDirRequestAddress(const std::string & dirRequestAddress);
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tapePool;
    std::string tapePoolAddress;
  };
  std::list<JobDump> dumpJobs();
  void garbageCollect(const std::string &presumedOwner);
};

}}
