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
#include "BackendVFS.hpp"
#include "common/exception/Exception.hpp"
#include "objectstore/SerializersExceptions.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "ArchiveQueue.hpp"

namespace unitTests {

TEST(ObjectStore, RootEntryBasicAccess) {
  cta::objectstore::BackendVFS be;
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
    cta::objectstore::ScopedExclusiveLock lock (re);
    // Create an agent
    cta::objectstore::Agent agent(be);
    agent.generateName("unitTest");
    re.fetch();
    cta::objectstore::EntryLog el("user0",
      "unittesthost", time(NULL));
    re.addOrGetAgentRegisterPointerAndCommit(agent, el);
    ASSERT_NO_THROW(re.getAgentRegisterAddress());
    re.commit();
    //agent.registerSelf();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_FALSE(re.exists());
}

TEST (ObjectStore, RootEntryArchiveQueues) {
  cta::objectstore::BackendVFS be;
    cta::objectstore::EntryLog el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.addOrGetAgentRegisterPointerAndCommit(ag, el);
  }
  ag.insertAndRegisterSelf();
  std::string tpAddr1, tpAddr2;
  {
    // Create the tape pools
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getArchiveQueueAddress("tapePool1"),
      cta::objectstore::RootEntry::NotAllocated);
    tpAddr1 = re.addOrGetArchiveQueueAndCommit("tapePool1", ag);
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
    tpAddr2 = re.addOrGetArchiveQueueAndCommit("tapePool2", ag);
    ASSERT_TRUE(be.exists(tpAddr2));
  }
  {
    // Remove the other pool
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    re.removeArchiveQueueAndCommit("tapePool2");
    ASSERT_FALSE(be.exists(tpAddr2));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeArchiveQueueAndCommit("tapePool1");
  ASSERT_FALSE(be.exists(tpAddr1));
  re.removeIfEmpty();
  ASSERT_FALSE(re.exists());
}

TEST (ObjectStore, RootEntryDriveRegister) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::EntryLog el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(ag, el);
  }
  ag.insertAndRegisterSelf();
  std::string driveRegisterAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(
      driveRegisterAddress = re.addOrGetDriveRegisterPointerAndCommit(ag, el));
    ASSERT_TRUE(be.exists(driveRegisterAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeDriveRegisterAndCommit();
    ASSERT_FALSE(be.exists(driveRegisterAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_FALSE(re.exists());
}

TEST(ObjectStore, RootEntryAgentRegister) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::EntryLog el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::Agent ag(be);
  ag.generateName("UnitTests");
  std::string arAddr;
  {
    // Create the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock(re);
    re.fetch();
    ASSERT_THROW(re.getAgentRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    arAddr = re.addOrGetAgentRegisterPointerAndCommit(ag, el);
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
    ASSERT_EQ(arAddr, re.addOrGetAgentRegisterPointerAndCommit(ag, el));
    // Remove it
    ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
    // Check that the object is gone
    ASSERT_FALSE(be.exists(arAddr));
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeIfEmpty();
  ASSERT_FALSE(re.exists());
}

TEST (ObjectStore, RootEntrySchedulerGlobalLock) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
    cta::objectstore::EntryLog el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::Agent ag(be);
  ag.initialize();
  ag.generateName("UnitTests");
  { 
    // Try to create the root entry and allocate the agent register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.addOrGetAgentRegisterPointerAndCommit(ag, el);
  }
  ag.insertAndRegisterSelf();
  std::string schedulerGlobalLockAddress;
  {
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    ASSERT_THROW(re.getDriveRegisterAddress(),
      cta::objectstore::RootEntry::NotAllocated);
    ASSERT_NO_THROW(
      schedulerGlobalLockAddress = re.addOrGetSchedulerGlobalLockAndCommit(ag, el));
    ASSERT_TRUE(be.exists(schedulerGlobalLockAddress));
  }
  {
    // delete the drive register
    // create the drive register
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    re.removeSchedulerGlobalLockAndCommit();
    ASSERT_FALSE(be.exists(schedulerGlobalLockAddress));
  }
  // Unregister the agent
  cta::objectstore::ScopedExclusiveLock agl(ag);
  ag.removeAndUnregisterSelf();
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.fetch();
  re.removeAgentRegisterAndCommit();
  re.removeIfEmpty();
  ASSERT_FALSE(re.exists());
}

}
