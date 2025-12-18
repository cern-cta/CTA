/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ArchiveQueue.hpp"

namespace cta::objectstore {

class ArchiveQueueShard : public ObjectOps<serializers::ArchiveQueueShard, serializers::ArchiveQueueShard_t> {
public:
  // Constructor with undefined address
  explicit ArchiveQueueShard(Backend& os);

  // Constructor
  ArchiveQueueShard(const std::string& address, Backend& os);

  // Upgrader form generic object
  explicit ArchiveQueueShard(GenericObject& go);

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
    uint64_t minArchiveRequestAge;
    time_t startTime;
    std::string mountPolicyName;
  };

  std::list<JobInfo> dumpJobs();

  struct JobsSummary {
    uint64_t jobs = 0;
    uint64_t bytes = 0;
  };

  JobsSummary getJobsSummary();

  /**
   * adds job, returns new size
   */
  uint64_t addJob(ArchiveQueue::JobToAdd& jobToAdd);

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

  ArchiveQueue::CandidateJobList
  getCandidateJobList(uint64_t maxBytes, uint64_t maxFiles, const std::set<std::string>& archiveRequestsToSkip);

  /** Re compute summaries in case they do not match the array content. */
  void rebuild();
};

}  // namespace cta::objectstore
