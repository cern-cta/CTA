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

#include "Backend.hpp"
#include "ObjectOps.hpp"
#include <string>
#include "objectstore/cta.pb.h"
#include "common/CreationLog.hpp"
#include "common/MountControl.hpp"
#include "ArchiveRequest.hpp"
#include "ArchiveRequest.hpp"
#include "EntryLogSerDeser.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {
  
class GenericObject;

class ArchiveQueue: public ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t> {
  // TODO: rename tapepoolname field to tapepool (including in probuf)
  
public:
  // Constructor
  ArchiveQueue(const std::string & address, Backend & os);
  
  // Undefined object constructor
  ArchiveQueue(Backend & os);
  
  // Upgrader form generic object
  ArchiveQueue(GenericObject & go);

  // In memory initialiser
  void initialize(const std::string & name);
  
  // Commit with sanity checks (override from ObjectOps
  void commit();
  
  // Set/get tape pool
  void setTapePool(const std::string & name);
  std::string getTapePool();
  
  // Archive jobs management ===================================================
  struct JobToAdd {
    ArchiveRequest::JobDump job;
    const std::string archiveRequestAddress;
    uint64_t archiveFileId;
    uint64_t fileSize;
    const cta::common::dataStructures::MountPolicy policy;
    time_t startTime;
  };
  void addJobsAndCommit(std::list<JobToAdd> & jobsToAdd);
  /// This version will check for existence of the job in the queue before
  // returns the count and sizes of actually added jobs (if any).
  struct AdditionSummary {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };
  AdditionSummary addJobsIfNecessaryAndCommit(std::list<JobToAdd> & jobsToAdd);
  
  struct JobsSummary {
    uint64_t files;
    uint64_t bytes;
    time_t oldestJobStartTime;
    uint64_t priority;
    uint64_t minArchiveRequestAge;
    uint64_t maxDrivesAllowed;
  };
  JobsSummary getJobsSummary();
  
  void removeJobsAndCommit(const std::list<std::string> & requestsToRemove);
  struct JobDump {
    uint64_t size;
    std::string address;
    uint16_t copyNb;
  };
  std::list<JobDump> dumpJobs();
  struct CandidateJobList {
    uint64_t remainingFilesAfterCandidates = 0;
    uint64_t remainingBytesAfterCandidates = 0;
    uint64_t candidateFiles = 0;
    uint64_t candidateBytes = 0;
    std::list<JobDump> candidates;
  };
  // The set of archive requests to skip are requests previously identified by the caller as bad,
  // which still should be removed from the queue. They will be disregarded from  listing.
  CandidateJobList getCandidateList(uint64_t maxBytes, uint64_t maxFiles, std::set<std::string> archiveRequestsToSkip);
   
  // Check that the tape pool is empty (of both tapes and jobs)
  bool isEmpty();
 
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  // Garbage collection
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  
  std::string dump();
};
  
}}
