
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
#include "common/log/DummyLogger.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif
#include "GarbageCollector.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "DriveRegister.hpp"
#include "ArchiveRequest.hpp"
#include "RetrieveRequest.hpp"
#include "ArchiveQueue.hpp"
#include "RetrieveQueue.hpp"
#include "EntryLogSerDeser.hpp"
#include "catalogue/DummyCatalogue.hpp"

namespace unitTests {

TEST(ObjectStore, GarbageCollectorBasicFuctionnality) {
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::catalogue::DummyCatalogue catalogue;
  cta::log::LogContext lc(dl);
  // Here we check for the ability to detect dead (but empty agents)
  // and clean them up.
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // Create 2 agents, A and B and register them
  // The agents are set with a timeout of 0, so they will be delclared
  // dead immediately.
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl), agrB("unitTestAgentB", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be), agB(agrB.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
  agB.initialize();
  agB.setTimeout_us(0);
  agB.insertAndRegisterSelf(lc);
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
}

TEST(ObjectStore, GarbageCollectorRegister) {
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
  }
  ASSERT_FALSE(be.exists(arName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
}

TEST(ObjectStore, GarbageCollectorArchiveQueue) {
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
}

TEST(ObjectStore, GarbageCollectorDriveRegister) {
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // Create an agent and add the drive register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
}

TEST(ObjectStore, GarbageCollectorArchiveRequest) {
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call ArchiveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);
  // Create an agent to garbage collected
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
  // Several use cases are present for the ArchiveRequests:
  // - just referenced in agent ownership list, but not yet created.
  // - just created but not linked to any tape pool
  // - partially linked to tape pools
  // - linked to all tape pools
  // - In the 2 latter cases, the job could have been picked up for processing
  //
  // Create 2 archive queues
  std::string tpAddr[2];
  for (int i=0; i<2; i++)
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::stringstream tapePoolName;
    tapePoolName << "TapePool" << i;
    tpAddr[i] = re.addOrGetArchiveQueueAndCommit(tapePoolName.str(), agentRef, cta::objectstore::QueueType::JobsToTransfer, lc);
    cta::objectstore::ArchiveQueue aq(tpAddr[i], be);
  }
  // Create the various ATFR's, stopping one step further each time.
  unsigned int pass=0;
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
    ar.addJob(1, "TapePool0", tpAddr[0], 1, 1);
    ar.addJob(2, "TapePool1", tpAddr[1], 1, 1);
    cta::common::dataStructures::MountPolicy mp;
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setArchiveErrorReportURL("");
    ar.setRequester(cta::common::dataStructures::UserIdentity("user0", "group0"));
    ar.setSrcURL("root://eoseos/myFile");
    ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    ar.insert();
    cta::objectstore::ScopedExclusiveLock atfrl(ar);
    if (pass < 2) { pass++; continue; }
    // The step is now deprecated
    if (pass < 3) { pass++; continue; }
    // - Referenced in the first tape pool
    {
      cta::objectstore::ArchiveQueue aq(tpAddr[0], be);
      cta::objectstore::ScopedExclusiveLock tpl(aq);
      aq.fetch();
      cta::objectstore::ArchiveRequest::JobDump jd;
      jd.copyNb = 1;
      jd.tapePool = "TapePool0";
      jd.owner = tpAddr[0];
      cta::common::dataStructures::MountPolicy policy;
      policy.archiveMinRequestAge = 0;
      policy.archivePriority = 1;
      policy.maxDrivesAllowed = 1;
      std::list <cta::objectstore::ArchiveQueue::JobToAdd> jta;
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000U+pass, policy, time(NULL)});
      aq.addJobsAndCommit(jta, agentRef, lc);
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
      jd.owner = tpAddr[1];
      cta::common::dataStructures::MountPolicy policy;
      policy.archiveMinRequestAge = 0;
      policy.archivePriority = 1;
      policy.maxDrivesAllowed = 1;
      std::list <cta::objectstore::ArchiveQueue::JobToAdd> jta;
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000+pass, policy, time(NULL)});
      aq.addJobsAndCommit(jta, agentRef, lc);
    }
    if (pass < 5) { pass++; continue; }
    // The step is now deprecated
    break;
  }
  // Create the garbage collector and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
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
    ASSERT_EQ(5, aq0.getJobsSummary().jobs);
    ASSERT_EQ(5, aq1.getJobsSummary().jobs);
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  // Remove jobs from archive queues
  std::list<std::string> tapePools = { "TapePool0", "TapePool1" };
  for (auto & tp: tapePools) {
    // Empty queue
    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tp, cta::objectstore::QueueType::JobsToTransfer), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    std::list<std::string> ajtr;
    for (auto &j: aq.dumpJobs()) {
      ajtr.push_back(j.address);
    }
    aq.removeJobsAndCommit(ajtr);
    aql.release();
    // Remove queues from root
    re.removeArchiveQueueAndCommit(tp, cta::objectstore::QueueType::JobsToTransfer, lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
  // TODO: this unit test still leaks tape pools and requests
}

TEST(ObjectStore, GarbageCollectorRetrieveRequest) {
  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call RetrieveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);
  // Create an agent to garbage be collected
  cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(lc);
  // Several use cases are present for the RetrieveRequests:
  // - just referenced in agent ownership list, but not yet created.
  // - just created but not linked to any tape
  // - partially linked to tape
  // - When requeueing the request, the tape could be disabled, in which case
  //   it will be deleted.
  //
  // Create 2 retrieve queues
  std::string tAddr[2];
  for (int i=0; i<2; i++)
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::stringstream vid;
    vid << "Tape" << i;
    tAddr[i] = re.addOrGetRetrieveQueueAndCommit(vid.str(), agentRef, cta::objectstore::QueueType::JobsToTransfer, lc);
    cta::objectstore::RetrieveQueue rq(tAddr[i], be);
  }
  // Create the various ATFR's, stopping one step further each time.
  int pass=0;
  while (true)
  {
    // -just referenced
    std::string atfrAddr = agrA.nextId("RetrieveRequest");
    agrA.addToOwnership(atfrAddr, be);
    if (pass < 1) { pass++; continue; }
    // - created, but not linked to tape pools. Those jobs will be queued by the garbage
    // collector.
    cta::objectstore::RetrieveRequest rr(atfrAddr, be);
    rr.initialize();
    cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
    rqc.archiveFile.archiveFileID = 123456789L;
    rqc.archiveFile.diskFileId = "eos://diskFile";
    rqc.archiveFile.checksumType = "";
    rqc.archiveFile.checksumValue = "";
    rqc.archiveFile.creationTime = 0;
    rqc.archiveFile.reconciliationTime = 0;
    rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    rqc.archiveFile.diskInstance = "eoseos";
    rqc.archiveFile.fileSize = 1000 + pass;
    rqc.archiveFile.storageClass = "sc";
    rqc.archiveFile.tapeFiles[1].blockId=0;
    rqc.archiveFile.tapeFiles[1].compressedSize=1;
    rqc.archiveFile.tapeFiles[1].compressedSize=1;
    rqc.archiveFile.tapeFiles[1].copyNb=1;
    rqc.archiveFile.tapeFiles[1].creationTime=time(nullptr);
    rqc.archiveFile.tapeFiles[1].fSeq=pass;
    rqc.archiveFile.tapeFiles[1].vid="Tape0";
    rqc.archiveFile.tapeFiles[2].blockId=0;
    rqc.archiveFile.tapeFiles[2].compressedSize=1;
    rqc.archiveFile.tapeFiles[2].compressedSize=1;
    rqc.archiveFile.tapeFiles[2].copyNb=2;
    rqc.archiveFile.tapeFiles[2].creationTime=time(nullptr);
    rqc.archiveFile.tapeFiles[2].fSeq=pass;
    rqc.archiveFile.tapeFiles[2].vid="Tape1";
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.maxDrivesAllowed = 1;
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time=time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1);
    rr.addJob(2, 1, 1);    
    rr.setOwner(agA.getAddressIfSet());
    rr.setActiveCopyNumber(0);
    rr.insert();
    cta::objectstore::ScopedExclusiveLock rrl(rr);
    if (pass < 3) { pass++; continue; }
    // - Reference job in the first tape
    {
      cta::objectstore::RetrieveQueue rq(tAddr[0], be);
      cta::objectstore::ScopedExclusiveLock rql(rq);
      rq.fetch();
      std::list <cta::objectstore::RetrieveQueue::JobToAdd> jta;
      jta.push_back({1,rqc.archiveFile.tapeFiles[1].fSeq, rr.getAddressIfSet(), rqc.archiveFile.fileSize, rqc.mountPolicy, sReq.creationLog.time});
      rq.addJobsAndCommit(jta, agentRef, lc);
    }
    if (pass < 5) { pass++; continue; }
    // - Still marked as not owned but referenced in the agent
    {
      rr.setOwner(tAddr[0]);
      rr.setActiveCopyNumber(1);
      rr.commit();
    }
    break;
  }
  // Mark the tape as enabled
  catalogue.addEnabledTape("Tape0");
  // Mark the other tape as disabled
  catalogue.addDisabledTape("Tape1");
  // Create the garbage collector and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
    gc.runOnePass(lc);
    gc.runOnePass(lc);
  }
  // All 4 requests should be linked in the first tape queue
  {
    cta::objectstore::RetrieveQueue rq(tAddr[0], be);
    cta::objectstore::ScopedExclusiveLock tp0lock(rq);
    rq.fetch();
    auto dump=rq.dumpJobs();
    // We expect all jobs with sizes 1002-1005 inclusive to be connected to
    // their respective tape pools.
    ASSERT_EQ(5, rq.getJobsSummary().files);
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(lc);
  // We should not be able to remove the agent register (as it should be empty)
  rel.lock(re);
  re.fetch();
  // Remove jobs from retrieve queue
  std::list<std::string> retrieveQueues = { "Tape0", "Tape1" };
  for (auto & vid: retrieveQueues) {
    // Empty queue
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(vid, cta::objectstore::QueueType::JobsToTransfer), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    std::list<std::string> jtrl;
    for (auto &j: rq.dumpJobs()) {
      jtrl.push_back(j.address);
    }
    rq.removeJobsAndCommit(jtrl);
    rql.release();
    // Remove queues from root
    re.removeRetrieveQueueAndCommit(vid, cta::objectstore::QueueType::JobsToTransfer, lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
  // TODO: this unit test still leaks tape pools and requests
}

}
