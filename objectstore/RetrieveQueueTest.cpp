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

#include <gtest/gtest.h>
#include "RetrieveQueue.hpp"
#include "BackendVFS.hpp"
#include "AgentReference.hpp"
#include "common/log/DummyLogger.hpp"

#include <random>

namespace unitTests {
  
TEST(ObjectStore, RetrieveQueueBasicAccess) {
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

TEST(ObjectStore, RetrieveQueueShardingAndOrderingTest) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agentRef("unitTest", dl);
  std::mt19937 gen((std::random_device())());
  // Create 1000 jobs references.
  std::list<cta::objectstore::RetrieveQueue::JobToAdd> jobsToAdd;
  for (size_t i=0; i<1000; i++) {
    cta::objectstore::RetrieveQueue::JobToAdd jta;
    jta.copyNb = 1;
    jta.fSeq = i;
    jta.fileSize = 1000;
    jta.policy.maxDrivesAllowed = 10;
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
    rq.setShardSize(20);
    rq.insert();
  }
  {
    // Read the queue and insert jobs 3 by 3 (the insertion size is 
    // expected to be << shard size (5 here).
    auto jobsToAddNow = jobsToAdd;
    while (jobsToAddNow.size()) {
      std::list<cta::objectstore::RetrieveQueue::JobToAdd> jobsBatch;
      for (size_t i=0; i<15; i++) {
        if (jobsToAddNow.size()) {
          auto j=std::next(jobsToAddNow.begin(), (std::uniform_int_distribution<size_t>(0, jobsToAddNow.size() -1))(gen));
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
    // have some efficiencies, but we expect at least an average 5 jobs per shard
    // (less than 500 shards).
    cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_LT(rq.getShardCount(), 100);
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
    while (rq.getJobsSummary().files) {
      auto candidateJobs = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 50, std::set<std::string>());
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
      auto candidateJobs2 = rq.getCandidateList(std::numeric_limits<uint64_t>::max(), 1, jobsToSkip);
      if (candidateJobs2.candidateFiles) {
        std::stringstream address;
        address << "someRequest-" << nextExpectedFseq;
        ASSERT_EQ(address.str(), candidateJobs2.candidates.front().address);
      }
      rq.removeJobsAndCommit(jobsToDelete);
    }
    ASSERT_EQ(nextExpectedFseq, 1000);
  }

  // Delete the root entry
  cta::objectstore::RetrieveQueue rq(retrieveQueueAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(rq);
  rq.fetch();
  rq.removeIfEmpty(lc);
  ASSERT_FALSE(rq.exists());
}

}
