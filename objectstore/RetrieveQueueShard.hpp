/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RetrieveQueue.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"

namespace cta::objectstore {

class RetrieveQueueShard : public ObjectOps<serializers::RetrieveQueueShard, serializers::RetrieveQueueShard_t> {
public:
  // Constructor with undefined address
  explicit RetrieveQueueShard(Backend& os);

  // Constructor
  RetrieveQueueShard(const std::string& address, Backend& os);

  // Upgrader form generic object
  explicit RetrieveQueueShard(GenericObject& go);

  // Forbid/hide base initializer
  void initialize() override;

  // Initializer
  void initialize(const std::string& owner);

  // dumper
  std::string dump();

  void garbageCollect(const std::string& presumedOwner,
                      AgentReference& agentReference,
                      log::LogContext& lc,
                      cta::catalogue::Catalogue& catalogue) override;

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
    constexpr bool operator()(const common::dataStructures::RetrieveJobToAdd& lhs,
                              const common::dataStructures::RetrieveJobToAdd& rhs) const {
      return lhs.fSeq < rhs.fSeq;
    }
  };

  using JobsToAddSet = std::multiset<common::dataStructures::RetrieveJobToAdd, RetrieveQueueJobsToAddLess>;

  struct RetrieveQueueSerializedJobsToAddLess {
    bool operator()(const serializers::RetrieveJobPointer& lhs, const serializers::RetrieveJobPointer& rhs) const {
      return lhs.fseq() < rhs.fseq();
    }
  };

  using SerializedJobsToAddSet = std::multiset<serializers::RetrieveJobPointer, RetrieveQueueSerializedJobsToAddLess>;

private:
  /**
   * adds batch of jobs.
   */
  void addJobsInPlace(JobsToAddSet& jobsToAdd);

  void addJobsThroughCopy(JobsToAddSet& jobsToAdd);

public:
  /**
   * adds job
   */
  void addJob(const common::dataStructures::RetrieveJobToAdd& jobToAdd);

  void addJobsBatch(JobsToAddSet& jobToAdd);

  struct RemovalResult {
    uint64_t jobsRemoved = 0;
    uint64_t jobsAfter = 0;
    uint64_t bytesRemoved = 0;
    uint64_t bytesAfter = 0;
    std::list<JobInfo> removedJobs;
  };

  /**
   * Removes jobs from shard (and from the to remove list). Returns list of removed jobs.
   */
  RemovalResult removeJobs(const std::list<std::string>& jobsToRemove);

  RetrieveQueue::CandidateJobList getCandidateJobList(uint64_t maxBytes,
                                                      uint64_t maxFiles,
                                                      const std::set<std::string>& retrieveRequestsToSkip,
                                                      const std::set<std::string>& diskSystemsToSkip);

  /** Re compute summaries in case they do not match the array content. */
  void rebuild();
};

}  // namespace cta::objectstore
