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

#include "objectstore/BackendRadosTestSwitch.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "objectstore/BackendRados.hpp"
#include "common/log/DummyLogger.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

#include <exception>
#include <gtest/gtest.h>
#include <algorithm>
#include <uuid/uuid.h>

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "common/log/StdoutLogger.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/GarbageCollector.hpp"
#include "objectstore/ObjectStoreFixture.hpp"
#include "objectstore/QueueCleanupRunner.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "scheduler/Scheduler.hpp"

#include "objectstore/QueueCleanupRunnerTestUtils.hpp"

//#define STDOUT_LOGGING

namespace unitTests {

using Tape = cta::common::dataStructures::Tape;

/**
 * This structure represents the state and number of jobs of a queue at a certain point.
 * It is used to parameterize the tests.
 */
struct ConcurrentTapeQueueSetup {
  uint32_t retrieveQueueToTransferJobs;
  uint32_t retrieveQueueToReportJobs;
};

/**
 * This structure represents the initial and final setup of a queue.
 * It is used to parameterize the tests.
 */
struct ConcurrentTapeQueueTransition {
  std::string vid;
  Tape::State initialState; // Initial tape state
  Tape::State desiredState; // New desired state (`modifyTapeState`)
  Tape::State expectedState; // Expected state at the end of test
  ConcurrentTapeQueueSetup initialSetup;
  ConcurrentTapeQueueSetup finalSetup;
};

/**
 * This structure parameterizes the initial number of requests to insert on a queue and the existing replicas.
 * It is used to parameterize the tests.
 */
struct RetrieveRequestSetup {
  uint32_t numberOfRequests;
  std::string activeCopyVid;
  std::list<std::string> replicaCopyVids;
};

/**
 * This structure is used to parameterize OStore database tests.
 */
struct QueueCleanupRunnerConcurrentTestParams {
  cta::SchedulerDatabaseFactory &dbFactory;
  std::list<RetrieveRequestSetup> &retrieveRequestSetupList;
  std::list<ConcurrentTapeQueueTransition> &tapeQueueTransitionList;
  double cleanupTimeout;

  QueueCleanupRunnerConcurrentTestParams(
          cta::SchedulerDatabaseFactory &dbFactory,
          std::list<RetrieveRequestSetup> &retrieveRequestSetupList,
          std::list<ConcurrentTapeQueueTransition> &tapeQueueTransitionList,
          double cleanupTimeout) :
          dbFactory(dbFactory),
          retrieveRequestSetupList(retrieveRequestSetupList),
          tapeQueueTransitionList(tapeQueueTransitionList),
          cleanupTimeout(cleanupTimeout) {
  }
};

/**
 * The OStore database test is a parameterized test.  It takes an
 * OStore database factory as a parameter.
 */
class QueueCleanupRunnerConcurrentTest: public
                              ::testing::TestWithParam<QueueCleanupRunnerConcurrentTestParams> {
public:

  QueueCleanupRunnerConcurrentTest() noexcept {
  }

  class FailedToGetDatabase: public std::exception {
  public:
    const char *what() const noexcept override {
      return "Failed to get scheduler database";
    }
  };

  class FailedToGetCatalogue: public std::exception {
  public:
    const char *what() const noexcept override {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const noexcept override {
      return "Failed to get scheduler";
    }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto &factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the OStore DB from the factory.
    auto osDb = factory.create(m_catalogue);
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface).
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osDb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osDb.release()));
    // Setup scheduler
    m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_db, 5, 2 * 1000 * 1000);
  }

  virtual void TearDown() {
    cta::objectstore::Helpers::flushRetrieveQueueStatisticsCache();
    m_scheduler.reset();
    m_db.reset();
    m_catalogue.reset();
  }

  cta::objectstore::OStoreDBWrapperInterface &getDb() {
    cta::objectstore::OStoreDBWrapperInterface *const ptr = m_db.get();
    if (nullptr == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  cta::catalogue::DummyCatalogue &getCatalogue() {
    cta::catalogue::DummyCatalogue *const ptr = dynamic_cast<cta::catalogue::DummyCatalogue*>(m_catalogue.get());
    if (nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::Scheduler &getScheduler() {

    cta::Scheduler *const ptr = m_scheduler.get();
    if (nullptr == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }

private:
  // Prevent copying
  QueueCleanupRunnerConcurrentTest(const QueueCleanupRunnerConcurrentTest &) = delete;

  // Prevent assignment
  QueueCleanupRunnerConcurrentTest & operator= (const QueueCleanupRunnerConcurrentTest &) = delete;
  std::unique_ptr<cta::objectstore::OStoreDBWrapperInterface> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
};

class OStoreDBWithAgentBroken : public cta::OStoreDBWithAgent {

public:
  class TriggeredException: public std::exception {
  public:
    const char *what() const noexcept override {
      return "Triggered exception";
    }
  };

  using OStoreDBWithAgent::OStoreDBWithAgent;
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> getNextRetrieveJobsToTransferBatch(
          const std::string & vid, uint64_t filesRequested, cta::log::LogContext &logContext) override {
    throw TriggeredException();
  }
};


TEST_P(QueueCleanupRunnerConcurrentTest, CleanupRunnerParameterizedTest) {
  using cta::common::dataStructures::JobQueueType;
  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue & catalogue = getCatalogue();
  // Object store
  cta::objectstore::OStoreDBWrapperInterface & oKOStore = getDb();
  // Broken object store, pointing to same as `oKOStore`
  auto brokenOStore = OStoreDBWithAgentBroken(oKOStore.getBackend(), oKOStore.getAgentReference(), catalogue, dl);
  // Backend
  auto & be = dynamic_cast<cta::objectstore::BackendVFS&>(oKOStore.getBackend());
  // Remove this comment to avoid cleaning the object store files on destruction, useful for debugging
  // be.noDeleteOnExit();
  // Scheduler
  cta::Scheduler & scheduler = getScheduler();
  // Dummy admin
  const cta::common::dataStructures::SecurityIdentity dummyAdmin;

  //AgentA for queueing
  cta::objectstore::AgentReference agentForSetupRef("AgentForSetup", dl);
  cta::objectstore::Agent agentForSetup(agentForSetupRef.getAgentAddress(), be);

  //AgentB for popping
  cta::objectstore::AgentReference agentForCleanupRef("AgentForCleanup", dl);
  cta::objectstore::Agent agentForCleanup(agentForCleanupRef.getAgentAddress(), be);

  // Create the root entry
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.fetch();

  // Create the agent register
  re.addOrGetAgentRegisterPointerAndCommit(agentForSetupRef, el, lc);
  re.addOrGetAgentRegisterPointerAndCommit(agentForCleanupRef, el, lc);
  rel.release();

  agentForSetup.initialize();
  agentForSetup.insertAndRegisterSelf(lc);
  agentForCleanup.initialize();
  agentForCleanup.insertAndRegisterSelf(lc);

  // Create retrieve requests and add them to the queues
  // Create queues when they do not exist
  for (auto & retrieveRequestSetupList : GetParam().retrieveRequestSetupList) {

    // Identify list of vids where copies exist, including active copy
    std::set<std::string> allVids;
    allVids.insert(retrieveRequestSetupList.replicaCopyVids.begin(), retrieveRequestSetupList.replicaCopyVids.end());
    allVids.insert(retrieveRequestSetupList.activeCopyVid);
    std::string activeVid = retrieveRequestSetupList.activeCopyVid;

    // Generate requests
    std::list<std::unique_ptr<cta::objectstore::RetrieveRequest> > requestsPtrs;
    cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue, cta::objectstore::RetrieveQueueToTransfer>::InsertedElement::list requests;
    unitTests::fillRetrieveRequestsForCleanupRunner(requests, retrieveRequestSetupList.numberOfRequests, requestsPtrs, allVids, activeVid, be, agentForSetupRef); //memory leak avoided here with 'requestsPtrs'

    // Create queue for requests to active copy
    std::string agentForSetupAddr = agentForSetupRef.getAgentAddress();
    {
      cta::objectstore::ScopedExclusiveLock relQ(re);
      re.fetch();
      re.addOrGetRetrieveQueueAndCommit(activeVid, agentForSetupRef, JobQueueType::JobsToTransferForUser);
    }

    // Insert requests into active vid queue
    using RetrieveQueueAlgorithm = cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue, cta::objectstore::RetrieveQueueToTransfer>;
    RetrieveQueueAlgorithm retrieveQueueAlgo(be, agentForSetupRef);
    retrieveQueueAlgo.referenceAndSwitchOwnership(activeVid, agentForSetupAddr, requests, lc);
  }

  // Setup initial tape states and validate number of requests
  //for (TapeQueueTransition tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {
  for (auto & tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {

    std::string vid = tapeQueueStateTrans.vid;
    auto initialState = tapeQueueStateTrans.initialState;
    auto initialRetrieveQueueToTransferJobs = tapeQueueStateTrans.initialSetup.retrieveQueueToTransferJobs;
    auto initialRetrieveQueueToReportJobs = tapeQueueStateTrans.initialSetup.retrieveQueueToReportJobs;

    // Initial tape state
    catalogue.Tape()->modifyTapeState(dummyAdmin, vid, initialState, std::nullopt, "Testing");

    // Assert initial queue setup, for pre-validation of tests
    {
      re.fetchNoLock();
      if (initialRetrieveQueueToTransferJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(initialRetrieveQueueToTransferJobs, rQueue.getJobsSummary().jobs);
      } else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser), cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
      if (initialRetrieveQueueToReportJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(initialRetrieveQueueToReportJobs, rQueue.getJobsSummary().jobs);
      } else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser), cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
    }
  }

  // Trigger tape state change
  for (auto & tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {

    std::string vid = tapeQueueStateTrans.vid;
    auto initialState = tapeQueueStateTrans.initialState;
    auto desiredState = tapeQueueStateTrans.desiredState;

    if (initialState == desiredState) {
      continue; // No desired tape state change, ignore
    }

    scheduler.triggerTapeStateChange(dummyAdmin, vid, desiredState, "", lc);
  }

  // Execute cleanup runner
  {
    cta::objectstore::QueueCleanupRunner qCleanupRunnerBroken(agentForCleanupRef, brokenOStore, catalogue, GetParam().cleanupTimeout);
    cta::objectstore::QueueCleanupRunner qCleanupRunnerOk(agentForCleanupRef, oKOStore, catalogue, GetParam().cleanupTimeout);

    ASSERT_THROW(qCleanupRunnerBroken.runOnePass(lc), OStoreDBWithAgentBroken::TriggeredException);
    ASSERT_NO_THROW(qCleanupRunnerOk.runOnePass(lc)); // Two passes are needed for the other cleanup runner to be able to track the heartbeats
    ASSERT_NO_THROW(qCleanupRunnerOk.runOnePass(lc));
  }

  // Validate final setup of tapes and corresponding queues, after the cleanup runner has been executed
  for (auto & tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {

    std::string vid = tapeQueueStateTrans.vid;
    auto expectedState = tapeQueueStateTrans.expectedState;
    auto expectedRetrieveQueueToTransferJobs = tapeQueueStateTrans.finalSetup.retrieveQueueToTransferJobs;
    auto expectedRetrieveQueueToReportJobs = tapeQueueStateTrans.finalSetup.retrieveQueueToReportJobs;

    // Check final tape state
    const auto tapeState = static_cast<cta::catalogue::DummyTapeCatalogue*>(catalogue.Tape().get())->getTapeState(vid);
    ASSERT_EQ(expectedState, tapeState);

    // Assert final queue setup
    {
      re.fetchNoLock();
      if (expectedRetrieveQueueToTransferJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(expectedRetrieveQueueToTransferJobs, rQueue.getJobsSummary().jobs);
      } else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser), cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
      if (expectedRetrieveQueueToReportJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(expectedRetrieveQueueToReportJobs, rQueue.getJobsSummary().jobs);
      } else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser), cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
    }
  }
}

static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

static std::list<RetrieveRequestSetup> Test_retrieveRequestSetupList = {
        { 10, "Tape0", { } },
};
static std::list<ConcurrentTapeQueueTransition> Test_tapeQueueTransitionList_Completed = {
        {
                "Tape0",
                Tape::ACTIVE, Tape::REPACKING, Tape::REPACKING,
                {10, 0}, { 0, 10 }
        },
};
static std::list<ConcurrentTapeQueueTransition> Test_tapeQueueTransitionList_Failed = {
        {
                "Tape0",
                Tape::ACTIVE, Tape::REPACKING, Tape::REPACKING_PENDING,
                { 10, 0 }, { 10, 0 }
        },
};

INSTANTIATE_TEST_CASE_P(OStoreTestVFS, QueueCleanupRunnerConcurrentTest,
                        ::testing::Values(
                                // With a timeout of 0.0s the 2nd cleanup runner will be able to complete the task after the 1st has failed
                                QueueCleanupRunnerConcurrentTestParams(
                                        OStoreDBFactoryVFS,
                                        Test_retrieveRequestSetupList,
                                        Test_tapeQueueTransitionList_Completed,
                                        0.0),
                                // With a timeout of 120.0s the 2nd cleanup runner will NOT immediately complete the task after the 1st has failed
                                QueueCleanupRunnerConcurrentTestParams(
                                        OStoreDBFactoryVFS,
                                        Test_retrieveRequestSetupList,
                                        Test_tapeQueueTransitionList_Failed,
                                        120.0)
                        )
);
}