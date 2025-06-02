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

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "ArchiveQueue.hpp"
#include "BackendVFS.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "objectstore/SerializersExceptions.hpp"
#include "RetrieveQueue.hpp"
#include "RootEntry.hpp"

#include "ObjectStoreFixture.hpp"

namespace unitTests {

TEST_F(ObjectStore, RootEntryBasicAccess) {
  cta::objectstore::BackendVFS be;
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  {
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  {
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    ASSERT_THROW(re.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedSharedLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    re.dump();
  }
  {
    // Try to allocate the agent register
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    // Create an agent
    cta::log::DummyLogger dl("dummy", "dummyLogger");
    cta::objectstore::AgentReference agentRef("unitTest", dl);
    cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
    re.fetch();
    cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
    re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
    ASSERT_NO_THROW(re.getAgentRegisterAddress());
    re.commit();
    // agent.registerSelf();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit(lc);
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

TEST_F(ObjectStore, RootEntryArchiveQueues) {
  using cta::common::dataStructures::JobQueueType;
  cta::objectstore::BackendVFS be;
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agr("UnitTests", dl);
  cta::objectstore::Agent ag(agr.getAgentAddress(), be);
  ag.initialize();
  {
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc);
  }
  ag.insertAndRegisterSelf(lc);
  std::string tpAddr1, tpAddr2;
  {
    // Create the tape pools
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getArchiveQueueAddress("tapePool1", JobQueueType::JobsToTransferForUser),
                 cta::objectstore::RootEntry::NoSuchArchiveQueue);
    tpAddr1 = re.addOrGetArchiveQueueAndCommit("tapePool1", agr, JobQueueType::JobsToTransferForUser);
    // Check that we car read it
    cta::objectstore::ArchiveQueue aq(tpAddr1, be);
    cta::objectstore::ScopedSharedLock aql(aq);
    ASSERT_NO_THROW(aq.fetch());
  }
  {
    // Create another pool
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    tpAddr2 = re.addOrGetArchiveQueueAndCommit("tapePool2", agr, JobQueueType::JobsToTransferForUser);
    ASSERT_TRUE(be.exists(tpAddr2));
  }
  {
    // Remove the other pool
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeArchiveQueueAndCommit("tapePool2", JobQueueType::JobsToTransferForUser, lc);
    ASSERT_FALSE(be.exists(tpAddr2));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf(lc);
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit(lc);
  re.removeArchiveQueueAndCommit("tapePool1", JobQueueType::JobsToTransferForUser, lc);
  ASSERT_FALSE(be.exists(tpAddr1));
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

TEST_F(ObjectStore, RootEntryDriveRegister) {
  cta::objectstore::BackendVFS be;
  {
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agr("UnitTests", dl);
  cta::objectstore::Agent ag(agr.getAgentAddress(), be);
  ag.initialize();
  {
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc);
  }
  ag.insertAndRegisterSelf(lc);
  std::string driveRegisterAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(), cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(driveRegisterAddress = re.addOrGetDriveRegisterPointerAndCommit(agr, el));
    ASSERT_TRUE(be.exists(driveRegisterAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeDriveRegisterAndCommit(lc);
    ASSERT_FALSE(be.exists(driveRegisterAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf(lc);
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit(lc);
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

TEST_F(ObjectStore, RootEntryAgentRegister) {
  cta::objectstore::BackendVFS be;
  {
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agr("UnitTests", dl);
  cta::objectstore::Agent ag(agr.getAgentAddress(), be);
  ag.initialize();
  std::string arAddr;
  {
    // Create the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getAgentRegisterAddress(), cta::objectstore::RootEntry::NotAllocated);
    arAddr = re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc);
    // Check that we car read it
    cta::objectstore::AgentRegister ar(arAddr, be);
    cta::objectstore::ScopedSharedLock arl(ar);
    ASSERT_NO_THROW(ar.fetch());
  }
  {
    // Delete the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    // Check that we still get the same agent register
    ASSERT_EQ(arAddr, re.getAgentRegisterAddress());
    ASSERT_EQ(arAddr, re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc));
    // Remove it
    ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
    // Check that the object is gone
    ASSERT_FALSE(be.exists(arAddr));
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

TEST_F(ObjectStore, RootEntrySchedulerGlobalLock) {
  cta::objectstore::BackendVFS be;
  {
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agr("UnitTests", dl);
  cta::objectstore::Agent ag(agr.getAgentAddress(), be);
  ag.initialize();
  {
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc);
  }
  ag.insertAndRegisterSelf(lc);
  std::string schedulerGlobalLockAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(), cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(schedulerGlobalLockAddress = re.addOrGetSchedulerGlobalLockAndCommit(agr, el));
    ASSERT_TRUE(be.exists(schedulerGlobalLockAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeSchedulerGlobalLockAndCommit(lc);
    ASSERT_FALSE(be.exists(schedulerGlobalLockAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf(lc);
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit(lc);
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

TEST_F(ObjectStore, RetrieveQueueToReportToRepackForSuccessRootEntryTest) {
  using cta::common::dataStructures::JobQueueType;
  cta::objectstore::BackendVFS be;
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::log::DummyLogger dl("dummy", "dummyLogger");
  cta::log::LogContext lc(dl);
  cta::objectstore::AgentReference agr("UnitTests", dl);
  cta::objectstore::Agent ag(agr.getAgentAddress(), be);
  ag.initialize();
  {
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.addOrGetAgentRegisterPointerAndCommit(agr, el, lc);
  }
  ag.insertAndRegisterSelf(lc);

  std::string tpAddr1, tpAddr2;
  {
    // Create the tape vid
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    // Try to retrieve a retrieve queue address that does not exist
    ASSERT_THROW(re.getRetrieveQueueAddress("VID1", JobQueueType::JobsToReportToRepackForSuccess),
                 cta::objectstore::RootEntry::NoSuchRetrieveQueue);

    tpAddr1 = re.addOrGetRetrieveQueueAndCommit("VID1", agr, JobQueueType::JobsToReportToRepackForSuccess);

    ASSERT_FALSE(re.isEmpty());
    // Check that we can read it
    cta::objectstore::RetrieveQueueToReportToRepackForSuccess aq(tpAddr1, be);
    cta::objectstore::ScopedSharedLock aql(aq);
    ASSERT_NO_THROW(aq.fetch());
    ASSERT_EQ(aq.getVid(), "VID1");
    ASSERT_TRUE(aq.isEmpty());
  }
  {
    // Create another VID
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    tpAddr2 = re.addOrGetRetrieveQueueAndCommit("VID2", agr, JobQueueType::JobsToReportToRepackForSuccess);
    ASSERT_TRUE(be.exists(tpAddr2));
  }
  {
    // Remove the other VID
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeRetrieveQueueAndCommit("VID2", JobQueueType::JobsToReportToRepackForSuccess, lc);
    ASSERT_FALSE(be.exists(tpAddr2));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf(lc);
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit(lc);
  re.removeRetrieveQueueAndCommit("VID1", JobQueueType::JobsToReportToRepackForSuccess, lc);
  ASSERT_FALSE(be.exists(tpAddr1));
  re.removeIfEmpty(lc);
  ASSERT_FALSE(re.exists());
}

}  // namespace unitTests
