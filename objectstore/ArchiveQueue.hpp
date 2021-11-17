/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
  // Trait to specify the type of jobs associated with this type of queue
  typedef cta::common::dataStructures::ArchiveJob job_t;

  // Constructor
  ArchiveQueue(const std::string & address, Backend & os);
  
  // Undefined object constructor
  ArchiveQueue(Backend & os);
  
  // Upgrader form generic object
  ArchiveQueue(GenericObject & go);

  // In memory initialiser
  void initialize(const std::string & name);
  
  // Commit with sanity checks (overload from ObjectOps)
  void commit();
private:
  // Validates all summaries are in accordance with each other.
  bool checkMapsAndShardsCoherency();
  
  // Rebuild from shards if something goes wrong.
  void rebuild();
  
  // Recompute oldest job creation time
  void recomputeOldestAndYoungestJobCreationTime();
  
public:
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
  /** Add the jobs to the queue. 
   * The lock will be used to mark the shards as locked (the lock is the same for 
   * the main object and the shard, the is no shared access.
   * As we potentially have to create new shard(s), we need access to the agent 
   * reference (to generate a non-colliding object name).
   * We will also log the shard creation (hence the context)
   */ 
  void addJobsAndCommit(std::list<JobToAdd> & jobsToAdd, AgentReference & agentReference, log::LogContext & lc);
  /// This version will check for existence of the job in the queue before
  // returns the count and sizes of actually added jobs (if any).
  struct AdditionSummary {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };
  AdditionSummary addJobsIfNecessaryAndCommit(std::list<JobToAdd> & jobsToAdd,
    AgentReference & agentReference, log::LogContext & lc);
  
  struct JobsSummary {
    uint64_t jobs{0};
    uint64_t bytes{0};
    time_t oldestJobStartTime{0};
    time_t youngestJobStartTime{0};
    uint64_t priority{0};
    uint64_t minArchiveRequestAge{0};
    std::map<std::string, uint64_t> mountPolicyCountMap;
  };
  JobsSummary getJobsSummary();
  
  void removeJobsAndCommit(const std::list<std::string> & jobsToRemove);
  struct JobDump {
    uint64_t size;
    std::string address;
    uint32_t copyNb;
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

  //! Return a summary of the number of jobs and number of bytes in the queue
  CandidateJobList getCandidateSummary();

  // Check that the tape pool is empty (of both tapes and jobs)
  bool isEmpty();
 
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  // Garbage collection
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  
  std::string dump();
  
  // The shard size. From experience, 100k is where we start to see performance difference,
  // but nothing prevents us from using a smaller size.
  // The performance will be roughly flat until the queue size reaches the square of this limit
  // (meaning the queue object updates start to take too much time).
  // with this current value of 25k, the performance should be roughly flat until 25k^2=625M.
  static const uint64_t c_maxShardSize = 25000;
};

class ArchiveQueueToTransferForUser: public ArchiveQueue { using ArchiveQueue::ArchiveQueue; };
class ArchiveQueueToReportForUser: public ArchiveQueue { using ArchiveQueue::ArchiveQueue; };
class ArchiveQueueFailed: public ArchiveQueue { using ArchiveQueue::ArchiveQueue; };
class ArchiveQueueToTransferForRepack: public ArchiveQueue{ using ArchiveQueue::ArchiveQueue; };
class ArchiveQueueToReportToRepackForSuccess : public ArchiveQueue{ using ArchiveQueue::ArchiveQueue; };
class ArchiveQueueToReportToRepackForFailure: public ArchiveQueue{ using ArchiveQueue::ArchiveQueue; };
  
}}
