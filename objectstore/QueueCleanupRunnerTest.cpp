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
#include "objectstore/QueueCleanupRunnerTestUtils.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/Scheduler.hpp"

//#define STDOUT_LOGGING

namespace unitTests {

using Tape = cta::common::dataStructures::Tape;

/**
 * This structure represents the state and number of jobs of a queue at a certain point.
 * It is used to parameterize the tests.
 */
struct TapeQueueSetup {
  Tape::State state;
  uint32_t retrieveQueueToTransferJobs;
  uint32_t retrieveQueueToReportJobs;
};

/**
 * This structure represents the initial and final setup of a queue.
 * It is used to parameterize the tests.
 */
struct TapeQueueTransition {
  std::string vid;
  TapeQueueSetup initialSetup;
  TapeQueueSetup finalSetup;
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
struct QueueCleanupRunnerTestParams {
  cta::SchedulerDatabaseFactory& dbFactory;
  std::list<RetrieveRequestSetup>& retrieveRequestSetupList;
  std::list<TapeQueueTransition>& tapeQueueTransitionList;

  explicit QueueCleanupRunnerTestParams(cta::SchedulerDatabaseFactory* dbFactory,
                                        std::list<RetrieveRequestSetup>& retrieveRequestSetupList,
                                        std::list<TapeQueueTransition>& tapeQueueTransitionList) :
  dbFactory(*dbFactory),
  retrieveRequestSetupList(retrieveRequestSetupList),
  tapeQueueTransitionList(tapeQueueTransitionList) {}
};

/**
 * The OStore database test is a parameterized test.  It takes an
 * OStore database factory as a parameter.
 */
class QueueCleanupRunnerTest : public ::testing::TestWithParam<QueueCleanupRunnerTestParams> {
public:
  QueueCleanupRunnerTest() noexcept {}

  class FailedToGetDatabase : public std::exception {
  public:
    const char* what() const noexcept override { return "Failed to get scheduler database"; }
  };

  class FailedToGetCatalogue : public std::exception {
  public:
    const char* what() const noexcept override { return "Failed to get catalogue"; }
  };

  class FailedToGetScheduler : public std::exception {
  public:
    const char* what() const noexcept override { return "Failed to get scheduler"; }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto& factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the OStore DB from the factory.
    auto osdb = factory.create(m_catalogue);
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface).
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.release()));
    // Setup scheduler
    m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_db, 5, 2 * 1000 * 1000);
  }

  virtual void TearDown() {
    cta::objectstore::Helpers::flushRetrieveQueueStatisticsCache();
    m_scheduler.reset();
    m_db.reset();
    m_catalogue.reset();
  }

  cta::objectstore::OStoreDBWrapperInterface& getDb() {
    cta::objectstore::OStoreDBWrapperInterface* const ptr = m_db.get();
    if (nullptr == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  cta::catalogue::DummyCatalogue& getCatalogue() {
    cta::catalogue::DummyCatalogue* const ptr = dynamic_cast<cta::catalogue::DummyCatalogue*>(m_catalogue.get());
    if (nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::Scheduler& getScheduler() {
    cta::Scheduler* const ptr = m_scheduler.get();
    if (nullptr == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }

  Tape::State m_finalTapeState;

private:
  // Prevent copying
  QueueCleanupRunnerTest(const QueueCleanupRunnerTest&) = delete;

  // Prevent assignment
  QueueCleanupRunnerTest& operator=(const QueueCleanupRunnerTest&) = delete;
  std::unique_ptr<cta::objectstore::OStoreDBWrapperInterface> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
};

TEST_P(QueueCleanupRunnerTest, CleanupRunnerParameterizedTest) {
  using cta::common::dataStructures::JobQueueType;
  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue& catalogue = getCatalogue();
  // Object store
  cta::objectstore::OStoreDBWrapperInterface& oStore = getDb();
  // Backend
  auto& be = dynamic_cast<cta::objectstore::BackendVFS&>(oStore.getBackend());
  // Remove this comment to avoid cleaning the object store files on destruction, useful for debugging
  // be.noDeleteOnExit();
  // Scheduler
  cta::Scheduler& scheduler = getScheduler();
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
  //re.initialize();

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
  for (auto retrieveRequestSetupList : GetParam().retrieveRequestSetupList) {
    // Identify list of vids where copies exist, including active copy
    std::set<std::string> allVIds;
    allVIds.insert(retrieveRequestSetupList.replicaCopyVids.begin(), retrieveRequestSetupList.replicaCopyVids.end());
    allVIds.insert(retrieveRequestSetupList.activeCopyVid);
    std::string activeVid = retrieveRequestSetupList.activeCopyVid;

    // Generate requests
    std::list<std::unique_ptr<cta::objectstore::RetrieveRequest>> requestsPtrs;
    cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,
                                          cta::objectstore::RetrieveQueueToTransfer>::InsertedElement::list requests;
    fillRetrieveRequestsForCleanupRunner(requests, retrieveRequestSetupList.numberOfRequests, requestsPtrs, allVIds,
                                         activeVid, be,
                                         agentForSetupRef);  //memory leak avoided here with 'requestsPtrs'

    // Create queue for requests to active copy
    std::string agentForSetupAddr = agentForSetupRef.getAgentAddress();
    {
      cta::objectstore::ScopedExclusiveLock relQ(re);
      re.fetch();
      re.addOrGetRetrieveQueueAndCommit(activeVid, agentForSetupRef, JobQueueType::JobsToTransferForUser);
    }

    // Insert requests into active vid queue
    using RetrieveQueueAlgorithm =
      cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue, cta::objectstore::RetrieveQueueToTransfer>;
    RetrieveQueueAlgorithm retrieveQueueAlgo(be, agentForSetupRef);
    retrieveQueueAlgo.referenceAndSwitchOwnership(activeVid, agentForSetupAddr, requests, lc);
  }

  // Setup initial tape states and validate number of requests
  for (auto tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {
    std::string vid = tapeQueueStateTrans.vid;
    auto initialState = tapeQueueStateTrans.initialSetup.state;
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
      }
      else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser),
                     cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
      if (initialRetrieveQueueToReportJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(initialRetrieveQueueToReportJobs, rQueue.getJobsSummary().jobs);
      }
      else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser),
                     cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
    }
  }

  // Trigger tape state change
  for (auto tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {
    std::string vid = tapeQueueStateTrans.vid;
    auto initialState = tapeQueueStateTrans.initialSetup.state;
    auto finalState = tapeQueueStateTrans.finalSetup.state;

    if (initialState == finalState) {
      continue;  // No desired tape state change, ignore
    }

    scheduler.triggerTapeStateChange(dummyAdmin, vid, finalState, "", lc);
  }

  // Execute cleanup runner
  {
    cta::objectstore::QueueCleanupRunner qcr(agentForCleanupRef, oStore, catalogue);
    qcr.runOnePass(lc);  // RUNNER
  }

  // Validate final setup of tapes and corresponding queues, after the cleanup runner has been executed
  for (auto tapeQueueStateTrans : GetParam().tapeQueueTransitionList) {
    std::string vid = tapeQueueStateTrans.vid;
    auto finalDesiredState = tapeQueueStateTrans.finalSetup.state;
    auto finalRetrieveQueueToTransferJobs = tapeQueueStateTrans.finalSetup.retrieveQueueToTransferJobs;
    auto finalRetrieveQueueToReportJobs = tapeQueueStateTrans.finalSetup.retrieveQueueToReportJobs;

    // Check final tape state
    const auto tapeState = static_cast<cta::catalogue::DummyTapeCatalogue*>(catalogue.Tape().get())->getTapeState(vid);
    ASSERT_EQ(finalDesiredState, tapeState);

    // Assert final queue setup
    {
      re.fetchNoLock();
      if (finalRetrieveQueueToTransferJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(finalRetrieveQueueToTransferJobs, rQueue.getJobsSummary().jobs);
      }
      else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser),
                     cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
      if (finalRetrieveQueueToReportJobs > 0) {
        auto qAddr = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser);
        cta::objectstore::RetrieveQueue rQueue(qAddr, be);
        rQueue.fetchNoLock();
        ASSERT_EQ(finalRetrieveQueueToReportJobs, rQueue.getJobsSummary().jobs);
      }
      else {
        ASSERT_THROW(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToReportToUser),
                     cta::objectstore::RootEntry::NoSuchRetrieveQueue);
      }
    }
  }
}

static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

// Testing requests without replicas

// Test A1: Requests removed from an ACTIVE to REPACKING queue when no replicas are available
std::list<RetrieveRequestSetup> TestA1_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA1_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::REPACKING, 0, 10}},
};

// Test A2: Requests removed from a DISABLED to REPACKING queue when no replicas are available
std::list<RetrieveRequestSetup> TestA2_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA2_tapeQueueTransitionList {
  {"Tape0", {Tape::DISABLED, 10, 0}, {Tape::REPACKING, 0, 10}},
};

// Test A3: Requests removed from an ACTIVE to BROKEN queue when no replicas are available
std::list<RetrieveRequestSetup> TestA3_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA3_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::BROKEN, 0, 10}},
};

// Test A4: Requests removed from a DISABLED to BROKEN queue when no replicas are available
std::list<RetrieveRequestSetup> TestA4_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA4_tapeQueueTransitionList {
  {"Tape0", {Tape::DISABLED, 10, 0}, {Tape::BROKEN, 0, 10}},
};

// Test A5: No requests removed from an ACTIVE queue
std::list<RetrieveRequestSetup> TestA5_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA5_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::ACTIVE, 10, 0}},
};

// Test A5: No requests removed from an DISABLED queue
std::list<RetrieveRequestSetup> TestA6_retrieveRequestSetupList {
  {10, "Tape0", {}}
};
std::list<TapeQueueTransition> TestA6_tapeQueueTransitionList {
  {"Tape0", {Tape::DISABLED, 10, 0}, {Tape::DISABLED, 10, 0}},
};

// Testing requests with double replicas

// Test B1: Requests moved from a REPACKING queue to an ACTIVE queue
std::list<RetrieveRequestSetup> TestB1_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1"}}
};
std::list<TapeQueueTransition> TestB1_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::REPACKING, 0, 0}},
  {"Tape1", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 10, 0}  }
};

// Test B2: Requests moved from a REPACKING queue to a DISABLED queue
std::list<RetrieveRequestSetup> TestB2_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1"}}
};
std::list<TapeQueueTransition> TestB2_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0},  {Tape::REPACKING, 0, 0}},
  {"Tape1", {Tape::DISABLED, 0, 0}, {Tape::DISABLED, 10, 0}}
};

// Test B3: Requests not moved from a REPACKING queue to an already BROKEN queue
std::list<RetrieveRequestSetup> TestB3_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1"}}
};
std::list<TapeQueueTransition> TestB3_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::REPACKING, 0, 10}},
  {"Tape1", {Tape::BROKEN, 0, 0},  {Tape::BROKEN, 0, 0}    }
};

// Test B4: Requests not moved from a REPACKING queue to an already REPACKING queue
std::list<RetrieveRequestSetup> TestB4_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1"}}
};
std::list<TapeQueueTransition> TestB4_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0},   {Tape::REPACKING, 0, 10}},
  {"Tape1", {Tape::REPACKING, 0, 0}, {Tape::REPACKING, 0, 0} }
};

// Testing requests with multiple replicas

// Test C1: Requests moved from a REPACKING queue to 2 ACTIVE queue
std::list<RetrieveRequestSetup> TestC1_retrieveRequestSetupList {
  {5, "Tape0", {"Tape1"}},
  {5, "Tape0", {"Tape2"}}
};
std::list<TapeQueueTransition> TestC1_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::REPACKING, 0, 0}},
  {"Tape1", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 5, 0}   },
  {"Tape2", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 5, 0}   }
};

// Test C1: Requests moved from a REPACKING queue to ACTIVE (higher priority comparing to DISABLED)
std::list<RetrieveRequestSetup> TestC2_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1", "Tape2", "Tape3"}}
};
std::list<TapeQueueTransition> TestC2_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 10, 0}, {Tape::REPACKING, 0, 0}},
  {"Tape1", {Tape::ACTIVE, 0, 0},  {Tape::DISABLED, 0, 0} },
  {"Tape2", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 10, 0}  },
  {"Tape3", {Tape::ACTIVE, 0, 0},  {Tape::DISABLED, 0, 0} }
};

// Test C2: Mix of multiple requests being moved around
std::list<RetrieveRequestSetup> TestC3_retrieveRequestSetupList {
  {10, "Tape0", {"Tape1"}         },
  {10, "Tape0", {"Tape1", "Tape2"}},
  {10, "Tape1", {"Tape2", "Tape3"}},
  {10, "Tape2", {"Tape3", "Tape4"}}
};
std::list<TapeQueueTransition> TestC3_tapeQueueTransitionList {
  {"Tape0", {Tape::ACTIVE, 20, 0}, {Tape::REPACKING, 0, 10}},
  {"Tape1", {Tape::ACTIVE, 10, 0}, {Tape::BROKEN, 0, 0}    },
  {"Tape2", {Tape::ACTIVE, 10, 0}, {Tape::DISABLED, 20, 0} },
  {"Tape3", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 10, 0}   },
  {"Tape4", {Tape::ACTIVE, 0, 0},  {Tape::ACTIVE, 0, 0}    }
};

INSTANTIATE_TEST_CASE_P(
  OStoreTestVFS,
  QueueCleanupRunnerTest,
  ::testing::Values(
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA1_retrieveRequestSetupList, TestA1_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA2_retrieveRequestSetupList, TestA2_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA3_retrieveRequestSetupList, TestA3_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA4_retrieveRequestSetupList, TestA4_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA5_retrieveRequestSetupList, TestA5_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestA6_retrieveRequestSetupList, TestA6_tapeQueueTransitionList),

    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestB1_retrieveRequestSetupList, TestB1_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestB2_retrieveRequestSetupList, TestB2_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestB3_retrieveRequestSetupList, TestB3_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestB4_retrieveRequestSetupList, TestB4_tapeQueueTransitionList),

    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestC1_retrieveRequestSetupList, TestC1_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS, TestC2_retrieveRequestSetupList, TestC2_tapeQueueTransitionList),
    QueueCleanupRunnerTestParams(&OStoreDBFactoryVFS,
                                 TestC3_retrieveRequestSetupList,
                                 TestC3_tapeQueueTransitionList)));
}  // namespace unitTests