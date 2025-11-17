/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "RetrieveQueue.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"

namespace cta::objectstore {

class RetrieveQueueShard: public ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t>  {
public:
  // Constructor with undefined address
  explicit RetrieveQueueShard(Backend& os);
  
  // Constructor
  RetrieveQueueShard(const std::string & address, Backend & os);
  
  // Upgrader form generic object
  explicit RetrieveQueueShard(GenericObject& go);
  
  // Forbid/hide base initializer
  void initialize() override;
  
  // Initializer
  void initialize(const std::string & owner);
  
  // dumper
  std::string dump();
  
  void garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc, cta::catalogue::Catalogue& catalogue) override;

  struct JobInfo {
    uint64_t size;
    std::string address;
    uint32_t copyNb;
    uint64_t priority;
    uint64_t minRetrieveRequestAge;
    time_t startTime;
    uint64_t fSeq;
    std::string mountPolicyName;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
    bool isRepackJob;
    bool isVerifyJob;
  };
  std::list<JobInfo> dumpJobs();
  
  /** Variant function allowing shard to shard job transfer (not needed) archives, 
   * which do not split. */
  std::list<common::dataStructures::RetrieveJobToAdd> dumpJobsToAdd();
  
  struct JobsSummary {
    uint64_t jobs = 0;
    uint64_t bytes = 0;
    uint64_t minFseq = 0;
    uint64_t maxFseq = 0;
  };
  JobsSummary getJobsSummary();
  
  struct RetrieveQueueJobsToAddLess {
    constexpr bool operator() (const common::dataStructures::RetrieveJobToAdd& lhs, 
      const common::dataStructures::RetrieveJobToAdd& rhs) const { return lhs.fSeq < rhs.fSeq; }
  };
  
  using JobsToAddSet = std::multiset<common::dataStructures::RetrieveJobToAdd, RetrieveQueueJobsToAddLess>;
  
  struct RetrieveQueueSerializedJobsToAddLess {
    bool operator() (const serializers::RetrieveJobPointer& lhs, 
      const serializers::RetrieveJobPointer& rhs) const { return lhs.fseq() < rhs.fseq(); }
  };
  typedef std::multiset<serializers::RetrieveJobPointer, 
    RetrieveQueueSerializedJobsToAddLess> SerializedJobsToAddSet;
private:
  /**
   * adds batch of jobs.
   */
  void addJobsInPlace(JobsToAddSet & jobsToAdd);
  
  void addJobsThroughCopy(JobsToAddSet & jobsToAdd);
  
public:
  /**
   * adds job
   */
  void addJob(const common::dataStructures::RetrieveJobToAdd & jobToAdd);
  
  void addJobsBatch(JobsToAddSet & jobToAdd);
   
  struct RemovalResult {
    uint64_t jobsRemoved = 0;
    uint64_t jobsAfter = 0;
    uint64_t bytesRemoved = 0;
    uint64_t bytesAfter = 0;
    uint64_t repackJobsRemoved = 0;
    uint64_t repackJobsAfter = 0;
    uint64_t verifyJobsRemoved = 0;
    uint64_t verifyJobsAfter = 0;
    std::list<JobInfo> removedJobs;
  };
  /**
   * Removes jobs from shard (and from the to remove list). Returns list of removed jobs.
   */
  RemovalResult removeJobs(const std::list<std::string> & jobsToRemove);
  
  RetrieveQueue::CandidateJobList getCandidateJobList(uint64_t maxBytes, uint64_t maxFiles,
    const std::set<std::string> & retrieveRequestsToSkip, const std::set<std::string> & diskSystemsToSkip);
  
  /** Re compute summaries in case they do not match the array content. */
  void rebuild();
  
};

} // namespace cta::objectstore
