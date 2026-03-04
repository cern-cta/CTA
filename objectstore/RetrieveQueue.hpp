/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ObjectOps.hpp"
#include "RetrieveActivityCountMap.hpp"
#include "RetrieveRequest.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"
#include "scheduler/RetrieveRequestDump.hpp"

#include "objectstore/cta.pb.h"

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;

class RetrieveQueue : public ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t> {
public:
  // Trait to specify the type of jobs associated with this type of queue
  using job_t = common::dataStructures::RetrieveJob;

  RetrieveQueue(const std::string& address, Backend& os);
  // Undefined object constructor
  explicit RetrieveQueue(Backend& os);
  explicit RetrieveQueue(GenericObject& go);
  void initialize(const std::string& vid);
  void commit() override;
  void getPayloadFromHeader() override;

private:
  // Validates all summaries are in accordance with each other.
  bool checkMapsAndShardsCoherency();

  // Rebuild from shards if something goes wrong.
  void rebuild();

public:
  void garbageCollect(const std::string& presumedOwner,
                      AgentReference& agentReference,
                      log::LogContext& lc,
                      cta::catalogue::Catalogue& catalogue) override;
  bool isEmpty();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void removeIfEmpty(log::LogContext& lc);
  std::string dump();

  // Retrieve jobs management ==================================================
  void addJobsAndCommit(std::list<common::dataStructures::RetrieveJobToAdd>& jobsToAdd,
                        AgentReference& agentReference,
                        log::LogContext& lc);

  // This version will check for existence of the job in the queue before
  // returns the count and sizes of actually added jobs (if any).
  struct AdditionSummary {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };

  AdditionSummary addJobsIfNecessaryAndCommit(std::list<common::dataStructures::RetrieveJobToAdd>& jobsToAdd,
                                              AgentReference& agentReference,
                                              log::LogContext& lc);

  struct JobsSummary {
    struct ActivityCount {
      std::string activity;
      uint64_t count;
    };

    struct SleepInfo {
      time_t sleepStartTime;
      std::string diskSystemName;
      uint64_t sleepTime;
    };

    uint64_t jobs;
    uint64_t bytes;
    time_t oldestJobStartTime;
    time_t youngestJobStartTime;
    uint64_t priority;
    uint64_t minRetrieveRequestAge;
    std::map<std::string, uint64_t> mountPolicyCountMap;
    std::list<ActivityCount> activityCounts;
    std::optional<SleepInfo> sleepInfo;

    JobsSummary()
        : jobs(0),
          bytes(0),
          oldestJobStartTime(0),
          youngestJobStartTime(0),
          priority(0),
          minRetrieveRequestAge(0) {}

    JobsSummary(uint64_t j,
                uint64_t b,
                time_t ojst,
                time_t yjst,
                uint64_t p,
                uint64_t mrra,
                const std::map<std::string, uint64_t>& mpcm,
                const std::list<ActivityCount>& ac,
                const std::optional<SleepInfo>& si)
        : jobs(j),
          bytes(b),
          oldestJobStartTime(ojst),
          youngestJobStartTime(yjst),
          priority(p),
          minRetrieveRequestAge(mrra),
          mountPolicyCountMap(mpcm),
          activityCounts(ac),
          sleepInfo(si) {}
  };

  JobsSummary getJobsSummary();

  struct JobDump {
    std::string address;
    uint32_t copyNb;
    uint64_t size;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
  };

  std::list<JobDump> dumpJobs();

  struct CandidateJobList {
    uint64_t remainingFilesAfterCandidates = 0;
    uint64_t remainingBytesAfterCandidates = 0;
    uint64_t candidateFiles = 0;
    uint64_t candidateBytes = 0;
    std::list<JobDump> candidates;
  };

  // The set of retrieve requests to skip are requests previously identified by the caller as bad,
  // which still should be removed from the queue. They will be disregarded from  listing.
  CandidateJobList getCandidateList(uint64_t maxBytes,
                                    uint64_t maxFiles,
                                    const std::set<std::string>& retrieveRequestsToSkip,
                                    const std::set<std::string>& diskSystemsToSkip,
                                    log::LogContext& lc);

  //! Return a summary of the number of jobs and number of bytes in the queue
  CandidateJobList getCandidateSummary();

  //! Return the mount policy names for the queue object
  std::list<std::string> getMountPolicyNames();

  void removeJobsAndCommit(const std::list<std::string>& jobsToRemove, log::LogContext& lc);

  bool getQueueCleanupDoCleanup();
  void setQueueCleanupDoCleanup(bool value = true);

  std::optional<std::string> getQueueCleanupAssignedAgent();
  void setQueueCleanupAssignedAgent(const std::string& agent);
  void clearQueueCleanupAssignedAgent();

  uint64_t getQueueCleanupHeartbeat();
  void tickQueueCleanupHeartbeat();

  // -- Generic parameters
  std::string getVid();

  // Support for sleep waiting free space (back pressure).
  // This data is queried through getJobsSummary().
  void setSleepForFreeSpaceStartTimeAndName(time_t time, const std::string& diskSystemName, uint64_t sleepTime);
  void resetSleepForFreeSpaceStartTime();

private:
  struct ShardForAddition {
    bool newShard = false;
    bool creationDone = false;
    bool splitDone = false;
    bool toSplit = false;
    ShardForAddition* splitDestination = nullptr;
    bool fromSplit = false;
    ShardForAddition* splitSource = nullptr;
    std::string address;
    uint64_t minFseq;
    uint64_t maxFseq;
    uint64_t jobsCount;
    std::list<common::dataStructures::RetrieveJobToAdd> jobsToAdd;
    size_t shardIndex = std::numeric_limits<size_t>::max();
  };

  void updateShardLimits(uint64_t fSeq, ShardForAddition& sfa);

  void addJobToShardAndMaybeSplit(common::dataStructures::RetrieveJobToAdd& jobToAdd,
                                  std::list<ShardForAddition>::iterator& shardForAddition,
                                  std::list<ShardForAddition>& shardList);

public:
  /** Helper function for unit tests: use smaller shard size to validate ordered insertion */
  void setShardSize(uint64_t shardSize);

  /** Helper function for unit tests: validate that we have the expected shards */
  std::list<std::string> getShardAddresses();

private:
  // The shard size. From experience, 100k is where we start to see performance difference,
  // but nothing prevents us from using a smaller size.
  // The performance will be roughly flat until the queue size reaches the square of this limit
  // (meaning the queue object updates start to take too much time).
  // with this current value of 25k, the performance should be roughly flat until 25k^2=625M.
  static const uint64_t c_defaultMaxShardSize = 25000;

  uint64_t m_maxShardSize = c_defaultMaxShardSize;
};

class RetrieveQueueToTransfer : public RetrieveQueue {
  using RetrieveQueue::RetrieveQueue;
};

class RetrieveQueueToReportForUser : public RetrieveQueue {
  using RetrieveQueue::RetrieveQueue;
};

class RetrieveQueueFailed : public RetrieveQueue {
  using RetrieveQueue::RetrieveQueue;
};

class RetrieveQueueToReportToRepackForSuccess : public RetrieveQueue {
  using RetrieveQueue::RetrieveQueue;
};

class RetrieveQueueToReportToRepackForFailure : public RetrieveQueue {
  using RetrieveQueue::RetrieveQueue;
};

}  // namespace cta::objectstore
