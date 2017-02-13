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
#include "common/dataStructures/ArchiveFile.hpp"
#include "GarbageCollector.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "DriveRegister.hpp"
#include "ArchiveRequest.hpp"
#include "ArchiveQueue.hpp"
#include "EntryLogSerDeser.hpp"

namespace unitTests {

TEST(ObjectStore, GarbageCollectorBasicFuctionnality) {
  // Here we check for the ability to detect dead (but empty agents)
  // and clean them up.
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector");
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el);
  rel.release();
  // Create 2 agents, A and B and register them
  // The agents are set with a timeout of 0, so they will be delclared
  // dead immediately.
  cta::objectstore::AgentReference agrA("unitTestAgentA"), agrB("unitTestAgentB");
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be), agB(agrB.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  agB.initialize();
  agB.setTimeout_us(0);
  agB.insertAndRegisterSelf();
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector");
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.removeAndUnregisterSelf();
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
  ASSERT_NO_THROW(re.removeIfEmpty());
}

TEST(ObjectStore, GarbageCollectorRegister) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector");
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA");
  agrA.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new agent register, owned by agA (by hand as it is not an usual
  // situation)
  std::string arName;
  {
    arName = agrA.nextId("AgentRegister");
    cta::objectstore::AgentRegister ar(arName, be);
    ar.initialize();
    ar.setOwner(agrA.getAgentAddress());
    agrA.addToOwnership(arName, be);
    ar.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector");
  gcAgentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  ASSERT_FALSE(be.exists(arName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.removeAndUnregisterSelf();
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
  ASSERT_NO_THROW(re.removeIfEmpty());
}

TEST(ObjectStore, GarbageCollectorArchiveQueue) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector");
  agentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA");
  agrA.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new agent register, owned by agA (by hand as it is not an usual
  // situation)
  std::string tpName;
  {
    tpName = agrA.nextId("ArchiveQueue");
    cta::objectstore::ArchiveQueue aq(tpName, be);
    aq.initialize("SomeTP");
    aq.setOwner(agA.getAddressIfSet());
    agrA.addToOwnership(tpName, be);
    aq.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector");
  gcAgentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.removeAndUnregisterSelf();
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
  ASSERT_NO_THROW(re.removeIfEmpty());
}

TEST(ObjectStore, GarbageCollectorDriveRegister) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector");
  agentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el);
  rel.release();
  // Create an agent and add the drive register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA");
  agrA.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new drive register, owned by agA (by hand as it is not an usual
  // situation)
  std::string tpName;
  {
    tpName = agrA.nextId("ArchiveQueue");
    cta::objectstore::DriveRegister dr(tpName, be);
    dr.initialize();
    dr.setOwner(agA.getAddressIfSet());
    agrA.addToOwnership(tpName, be);
    dr.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector");
  gcAgentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.removeAndUnregisterSelf();
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
  ASSERT_NO_THROW(re.removeIfEmpty());
}

TEST(ObjectStore, GarbageCollectorArchiveRequest) {
  // Here we check that can successfully call ArchiveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector");
  agentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el);
  rel.release();
  // Create an agent
  cta::objectstore::AgentReference agrA("unitTestAgentA");
  agrA.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Several use cases are present for the ArchiveRequests:
  // - just referenced in agent ownership list, but not yet created.
  // - just created but not linked to any tape pool
  // - partially linked to tape pools
  // - linked to all tape pools
  // - In the 2 latter cases, the job could have been picked up for processing
  // - already
  //w
  // Create 2 tape pools (not owned).
  std::string tpAddr[2];
  for (int i=0; i<2; i++)
  {
    std::stringstream aqid;
    aqid << "ArchiveQueue" << i;
    tpAddr[i] = agentRef.nextId(aqid.str());
    cta::objectstore::ArchiveQueue aq(tpAddr[i], be);
    aq.initialize(aqid.str());
    aq.setOwner("");
    aq.insert();
  }
  // Create the various ATFR's, stopping one step further each time.
  int pass=0;
  while (true)
  {
    // -just referenced
    std::string atfrAddr = agrA.nextId("ArchiveRequest");
    agrA.addToOwnership(atfrAddr, be);
    if (pass < 1) { pass++; continue; }
    // - created, but not linked to tape pools. Those jobs will be queued by the garbage
    // collector.
    cta::objectstore::ArchiveRequest ar(atfrAddr, be);
    ar.initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = 123456789L;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumType = "";
    aFile.checksumValue = "";
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    ar.setArchiveFile(aFile);
    ar.addJob(1, "ArchiveQueue0", tpAddr[0], 1, 1);
    ar.addJob(2, "ArchiveQueue1", tpAddr[1], 1, 1);    
    ar.setOwner(agA.getAddressIfSet());
    cta::common::dataStructures::MountPolicy mp;
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setRequester(cta::common::dataStructures::UserIdentity("user0", "group0"));
    ar.setSrcURL("root://eoseos/myFile");
    ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    ar.insert();
    cta::objectstore::ScopedExclusiveLock atfrl(ar);
    if (pass < 2) { pass++; continue; }
    // - Change the jobs statuses from PendingNSCreation to LinkingToArchiveQueue.
    // They will be automatically connected to the tape pool by the garbage 
    // collector from that moment on.
    {
      ar.setAllJobsLinkingToArchiveQueue();
      ar.commit();
    }
    if (pass < 3) { pass++; continue; }
    // - Referenced in the first tape pool
    {
      cta::objectstore::ArchiveQueue aq(tpAddr[0], be);
      cta::objectstore::ScopedExclusiveLock tpl(aq);
      aq.fetch();
      cta::objectstore::ArchiveRequest::JobDump jd;
      jd.copyNb = 1;
      jd.tapePool = "TapePool0";
      jd.ArchiveQueueAddress = tpAddr[0];
      cta::common::dataStructures::MountPolicy policy;
      policy.archiveMinRequestAge = 0;
      policy.archivePriority = 1;
      policy.maxDrivesAllowed = 1;
      aq.addJob(jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000+pass, policy, time(NULL));
      aq.commit();
    }
    if (pass < 4) { pass++; continue; }
    // TODO: partially migrated or selected
    // - Referenced in the second tape pool
    {
      cta::objectstore::ArchiveQueue aq(tpAddr[1], be);
      cta::objectstore::ScopedExclusiveLock tpl(aq);
      aq.fetch();
      cta::objectstore::ArchiveRequest::JobDump jd;
      jd.copyNb = 2;
      jd.tapePool = "TapePool1";
      jd.ArchiveQueueAddress = tpAddr[1];
      cta::common::dataStructures::MountPolicy policy;
      policy.archiveMinRequestAge = 0;
      policy.archivePriority = 1;
      policy.maxDrivesAllowed = 1;
      aq.addJob(jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000+pass, policy, time(NULL));
      aq.commit();
    }
    if (pass < 5) { pass++; continue; }
    // - Still marked a not owned but referenced in the agent
    {
      ar.setOwner("");
      ar.commit();
    }
    break;
  }
  // Create the garbage collector and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector");
  gcAgentRef.setQueueFlushTimeout(std::chrono::milliseconds(0));
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  // All 4 requests should be linked in both tape pools
  {
    cta::objectstore::ArchiveQueue aq0(tpAddr[0], be);
    cta::objectstore::ScopedExclusiveLock tp0lock(aq0);
    aq0.fetch();
    auto d0=aq0.dumpJobs();
    cta::objectstore::ArchiveQueue aq1(tpAddr[1], be);
    cta::objectstore::ScopedExclusiveLock tp1lock(aq1);
    aq1.fetch();
    auto d1=aq1.dumpJobs();
    // We expect all jobs with sizes 1002-1005 inclusive to be connected to
    // their respective tape pools.
    ASSERT_EQ(5, aq0.getJobsSummary().files);
    ASSERT_EQ(5, aq1.getJobsSummary().files);
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.removeAndUnregisterSelf();
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit());
  ASSERT_NO_THROW(re.removeIfEmpty());
  // TODO: this unit test still leaks tape pools and requests
}

}
