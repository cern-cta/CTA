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

#include <gtest/gtest.h>
#include "RetrieveQueue.hpp"
#include "RetrieveQueueShard.hpp"
#include "BackendVFS.hpp"
#include "AgentReference.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"

#include <random>

#include "ObjectStoreFixture.hpp"

namespace unitTests {

TEST_F(ObjectStore, RetrieveQueueBasicAccess) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agentRef("unitTest", dl);
  std::string retrieveQueueAddress = agentRef.nextId("RetrieveQueue");
  {
    // Try to create the retrieve queue
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    rq.initialize("V12345");
    rq.insert();
  }
  {
    // Try to read back and dump the tape
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    ASSERT_THROW(rq.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedSharedLock lock(rq);
    ASSERT_NO_THROW(rq.fetch());
    rq.dump();
  }
  // Delete the queue entry
  cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(rq);
  rq.fetch();
  rq.removeIfEmpty(lc);
  ASSERT_FALSE(rq.exists());
}

TEST_F(ObjectStore, RetrieveQueueShardingAndOrderingTest) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agentRef("unitTest", dl);
  std::mt19937 gen((std::random_device())());
  // Create 1000 jobs references.
  std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsToAdd;
  const size_t totalJobs = 1000, shardSize=25, batchSize=10;
  for (size_t i=0; i<totalJobs; i++) {
    cta::common::dataStructures::RetrieveJobToAdd jta;
    jta.copyNb = 1;
    jta.fSeq = i;
    jta.fileSize = 1000;
    jta.policy.retrieveMinRequestAge = 10;
    jta.policy.retrievePriority = 1;
    jta.startTime = ::time(nullptr);
    std::stringstream address;
    address << "someRequest-" << i;
    jta.retrieveRequestAddress = address.str();
    jobsToAdd.push_back(jta);
  }
  // By construction, first job has lowest start time.
  auto minStartTime=jobsToAdd.front().startTime;
  std::string retrieveQueueAddress = agentRef.nextId("RetrieveQueue");
  {
    // Try to create the retrieve queue
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    rq.initialize("V12345");
    // Set a small shard size to validate multi shard behaviors
    rq.setShardSize(shardSize);
    rq.insert();
  }
  {
    // Read the queue and insert jobs 10 by 10 (the insertion size is
    // expected to be << shard size (25 here).
    auto jobsToAddNow = jobsToAdd;
    while (jobsToAddNow.size()) {
      std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsBatch;
      for (size_t i=0; i<batchSize; i++) {
        if (jobsToAddNow.size()) {
          std::uniform_int_distribution<size_t> distrib(0, jobsToAddNow.size() - 1);
          auto j = std::next(jobsToAddNow.begin(), distrib(gen));
          jobsBatch.emplace_back(*j);
          jobsToAddNow.erase(j);
        }
      }
      cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
      cta::objectstore::ScopedExclusiveLock rql(rq);
      rq.fetch();
      rq.addJobsAndCommit(jobsBatch, agentRef, lc);
    }
    // Check the shard count is not too high. Due to random insertion, we might
    // have some efficiencies, but we expect at least half full shards).
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_LT(rq.getShardAddresses().size(), totalJobs / shardSize * 2);
  }
  {
    // Try to read back
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    ASSERT_THROW(rq.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedExclusiveLock lock(rq);
    ASSERT_NO_THROW(rq.fetch());
    // Pop jobs while we can. They should come out in fseq order as there is
    // no interleaved push and pop.
    ASSERT_EQ(minStartTime, rq.getJobsSummary().oldestJobStartTime);
    uint64_t nextExpectedFseq=0;
    while (rq.getJobsSummary().jobs) {
      auto candidateJobs = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 50, std::set<std::string>(),
          std::set<std::string>(), lc);
      std::set<std::string> jobsToSkip;
      std::list<std::string> jobsToDelete;
      for (auto &j: candidateJobs.candidates) {
        std::stringstream address;
        address << "someRequest-" << nextExpectedFseq;
        ASSERT_EQ(address.str(), j.address);
        jobsToSkip.insert(j.address);
        jobsToDelete.emplace_back(j.address);
        nextExpectedFseq++;
      }
      auto candidateJobs2 = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 1, jobsToSkip, std::set<std::string>(), lc);
      if (candidateJobs2.candidateFiles) {
        std::stringstream address;
        address << "someRequest-" << nextExpectedFseq;
        ASSERT_EQ(address.str(), candidateJobs2.candidates.front().address);
      }
      rq.removeJobsAndCommit(jobsToDelete, lc);
    }
    ASSERT_EQ(nextExpectedFseq, totalJobs);
  }

  // Delete the root entry
  cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(rq);
  rq.fetch();
  rq.removeIfEmpty(lc);
  ASSERT_FALSE(rq.exists());
}

TEST_F(ObjectStore, RetrieveQueueMissingShardingTest) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agentRef("unitTest", dl);
  std::mt19937 gen((std::random_device())());
  // Create 100 jobs references.
  std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsToAdd;
  const size_t totalJobs = 100, shardSize=25, batchSize=10;
  uint64_t removedShardJobs;
  for (size_t i=0; i<totalJobs; i++) {
    cta::common::dataStructures::RetrieveJobToAdd jta;
    jta.copyNb = 1;
    jta.fSeq = i;
    jta.fileSize = 1000;
    jta.policy.retrieveMinRequestAge = 10;
    jta.policy.retrievePriority = 1;
    jta.startTime = ::time(nullptr);
    std::stringstream address;
    address << "someRequest-" << i;
    jta.retrieveRequestAddress = address.str();
    jobsToAdd.push_back(jta);
  }
  std::string retrieveQueueAddress = agentRef.nextId("RetrieveQueue");
  {
    // Try to create the retrieve queue
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    rq.initialize("V12345");
    // Set a small shard size to validate multi shard behaviors
    rq.setShardSize(shardSize);
    rq.insert();
  }
  {
    // Insert jobs into the queue
    // By inserting in batches, we guarantee that various shards will be created
    auto jobsToAddNow = jobsToAdd;
    while (jobsToAddNow.size()) {
      std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsBatch;
      for (size_t i = 0; i < batchSize; i++) {
        if (jobsToAddNow.size()) {
          jobsBatch.emplace_back(jobsToAddNow.front());
          jobsToAddNow.pop_front();
        }
      }
      cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
      cta::objectstore::ScopedExclusiveLock rql(rq);
      rq.fetch();
      rq.addJobsAndCommit(jobsBatch, agentRef, lc);
    }
  }
  {
    // Remove one shard object and leave hanging reference
    // Some requests will be lost
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    auto shardAddresses = rq.getShardAddresses();
    auto firstShardAddr = shardAddresses.front();
    cta::objectstore::RetrieveQueueShard rqs(shardAddresses.front(), be);
    rql.includeSubObject(rqs);
    rqs.fetchNoLock();
    removedShardJobs = rqs.dumpJobs().size();
    // Check that at least one job will be lost with the missing shard, but not the total of jobs enqueued
    ASSERT_GT(removedShardJobs, 0);
    ASSERT_LT(removedShardJobs, totalJobs);
    rqs.remove();
    // Assert that shard no longer exists
    ASSERT_THROW(rqs.fetchNoLock(), cta::exception::NoSuchObject);
  }
  {
    // Try to read back
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    cta::objectstore::ScopedExclusiveLock lock(rq);
    rq.fetch();
    uint64_t jobCount=0;
    while (rq.getJobsSummary().jobs) {
      cta::objectstore::RetrieveQueue::CandidateJobList candidateJobs;
      ASSERT_NO_THROW(candidateJobs =
              rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 50, std::set<std::string>(), std::set<std::string>(), lc));
      std::list<std::string> jobsToDelete;
      for (auto &j: candidateJobs.candidates) {
        jobsToDelete.emplace_back(j.address);
        jobCount++;
      }
      ASSERT_NO_THROW(rq.removeJobsAndCommit(jobsToDelete, lc));
    }
    ASSERT_EQ(jobCount, totalJobs - removedShardJobs);
  }

  // Delete the root entry
  cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(rq);
  rq.fetch();
  rq.removeIfEmpty(lc);
  ASSERT_FALSE(rq.exists());
}

TEST_F(ObjectStore, RetrieveQueueActivityCounts) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agentRef("unitTest", dl);
  std::mt19937 gen((std::random_device())());
  // Create 1000 jobs references.
  std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsToAdd;
  const size_t totalJobs = 100, shardSize=25, batchSize=10;
  for (size_t i=0; i<totalJobs; i++) {
    cta::common::dataStructures::RetrieveJobToAdd jta;
    jta.copyNb = 1;
    jta.fSeq = i;
    jta.fileSize = 1000;
    jta.policy.retrieveMinRequestAge = 10;
    jta.policy.retrievePriority = 1;
    jta.startTime = ::time(nullptr);
    std::stringstream address;
    address << "someRequest-" << i;
    jta.retrieveRequestAddress = address.str();
    // Some (but not all) jobs will be assigned an activity (and weight).
    if (!(i % 3)) {
      std::string activity;
      if (!(i % 2)) {
        activity = "A";
      } else {
        activity = "B";
      }
      jta.activity = activity;
    }
    jobsToAdd.push_back(jta);
  }
  // By construction, first job has lowest start time.
  auto minStartTime=jobsToAdd.front().startTime;
  std::string retrieveQueueAddress = agentRef.nextId("RetrieveQueue");
  {
    // Try to create the retrieve queue
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    rq.initialize("V12345");
    // Set a small shard size to validate multi shard behaviors
    rq.setShardSize(shardSize);
    rq.insert();
  }
  {
    // Read the queue and insert jobs 10 by 10 (the insertion size is
    // expected to be << shard size (25 here).
    auto jobsToAddNow = jobsToAdd;
    while (jobsToAddNow.size()) {
      std::list<cta::common::dataStructures::RetrieveJobToAdd> jobsBatch;
      for (size_t i=0; i<batchSize; i++) {
        if (jobsToAddNow.size()) {
          std::uniform_int_distribution<size_t> distrib(0, jobsToAddNow.size() -1);
          auto j = std::next(jobsToAddNow.begin(), distrib(gen));
          jobsBatch.emplace_back(*j);
          jobsToAddNow.erase(j);
        }
      }
      cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
      cta::objectstore::ScopedExclusiveLock rql(rq);
      rq.fetch();
      rq.addJobsAndCommit(jobsBatch, agentRef, lc);
    }
  }
  {
    // Try to read back
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    ASSERT_THROW(rq.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedExclusiveLock lock(rq);
    ASSERT_NO_THROW(rq.fetch());
    // Pop jobs while we can. They should come out in fseq order as there is
    // no interleaved push and pop.
    auto jobsSummary = rq.getJobsSummary();
    ASSERT_EQ(minStartTime, jobsSummary.oldestJobStartTime);
    // File fSeqs are in [0, 99], 34 multiples of 3 (0 included) odds are activity A, evens are B, 17 each.
    ASSERT_EQ(2, jobsSummary.activityCounts.size());
    typedef decltype(jobsSummary.activityCounts.front()) acCount;
    auto jsA = std::find_if(jobsSummary.activityCounts.begin(), jobsSummary.activityCounts.end(), [](const acCount &ac){return ac.activity == "A"; });
    ASSERT_NE(jobsSummary.activityCounts.end(), jsA);
    ASSERT_EQ(17, jsA->count);
    auto jsB = std::find_if(jobsSummary.activityCounts.begin(), jobsSummary.activityCounts.end(), [](const acCount &ac){return ac.activity == "B"; });
    ASSERT_NE(jobsSummary.activityCounts.end(), jsB);
    ASSERT_EQ(17, jsB->count);
    uint64_t nextExpectedFseq=0;
    while (rq.getJobsSummary().jobs) {
      auto candidateJobs = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 50, std::set<std::string>(),
          std::set<std::string>(), lc);
      std::set<std::string> jobsToSkip;
      std::list<std::string> jobsToDelete;
      for (auto &j: candidateJobs.candidates) {
        std::stringstream address;
        address << "someRequest-" << nextExpectedFseq;
        ASSERT_EQ(address.str(), j.address);
        jobsToSkip.insert(j.address);
        jobsToDelete.emplace_back(j.address);
        nextExpectedFseq++;
      }
      auto candidateJobs2 = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 1, jobsToSkip, std::set<std::string>(), lc);
      if (candidateJobs2.candidateFiles) {
        std::stringstream address;
        address << "someRequest-" << nextExpectedFseq;
        ASSERT_EQ(address.str(), candidateJobs2.candidates.front().address);
      }
      rq.removeJobsAndCommit(jobsToDelete, lc);
      // We should empty the queue in 2 rounds. After the first one, we get the jobs 0-49 out.
      auto jobsSummary2 = rq.getJobsSummary();
      if (jobsSummary2.jobs) {
        auto jsA2 = std::find_if(jobsSummary2.activityCounts.begin(), jobsSummary2.activityCounts.end(), [](const acCount &ac){return ac.activity == "A"; });
        ASSERT_NE(jobsSummary2.activityCounts.end(), jsA2);
        ASSERT_EQ(8, jsA2->count);
        auto jsB2 = std::find_if(jobsSummary2.activityCounts.begin(), jobsSummary2.activityCounts.end(), [](const acCount &ac){return ac.activity == "B"; });
        ASSERT_NE(jobsSummary2.activityCounts.end(), jsB2);
        ASSERT_EQ(9, jsB2->count);
      } else {
        // Of course, we should have no activity.
        ASSERT_EQ(0, jobsSummary2.activityCounts.size());
      }
    }
    ASSERT_EQ(nextExpectedFseq, totalJobs);
  }

  // Delete the root entry
  cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(rq);
  rq.fetch();
  rq.removeIfEmpty(lc);
  ASSERT_FALSE(rq.exists());
}

}
