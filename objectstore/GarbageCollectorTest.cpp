
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
#include "ArchiveRequest.hpp"
#include "BackendVFS.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif
#include "common/log/StringLogger.hpp"
#include "DriveRegister.hpp"
#include "EntryLogSerDeser.hpp"
#include "GarbageCollector.hpp"
#include "RetrieveQueue.hpp"
#include "RetrieveRequest.hpp"
#include "RootEntry.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"

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
  using cta::common::dataStructures::JobQueueType;
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
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(NULL));
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
    tpAddr[i] = re.addOrGetArchiveQueueAndCommit(tapePoolName.str(), agentRef, JobQueueType::JobsToTransferForUser);
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
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    ar.setArchiveFile(aFile);
    ar.addJob(1, "TapePool0", agrA.getAgentAddress(), 1, 1, 1);
    ar.addJob(2, "TapePool1", agrA.getAgentAddress(), 1, 1, 1);
    cta::common::dataStructures::MountPolicy mp;
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setArchiveErrorReportURL("");
    ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
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
      std::list <cta::objectstore::ArchiveQueue::JobToAdd> jta;
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000U+pass, policy, time(NULL)});
      aq.addJobsAndCommit(jta, agentRef, lc);
      ar.setJobOwner(1, aq.getAddressIfSet());
      ar.commit();
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
      std::list <cta::objectstore::ArchiveQueue::JobToAdd> jta;
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000+pass, policy, time(NULL)});
      aq.addJobsAndCommit(jta, agentRef, lc);
      ar.setJobOwner(2, aq.getAddressIfSet());
      ar.commit();
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
    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tp, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    std::list<std::string> ajtr;
    for (auto &j: aq.dumpJobs()) {
      ajtr.push_back(j.address);
    }
    aq.removeJobsAndCommit(ajtr);
    aql.release();
    // Remove queues from root
    re.removeArchiveQueueAndCommit(tp, JobQueueType::JobsToTransferForUser, lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
  // TODO: this unit test still leaks tape pools and requests
}

TEST(ObjectStore, GarbageCollectorRetrieveRequest) {
  using cta::common::dataStructures::JobQueueType;
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
    tAddr[i] = re.addOrGetRetrieveQueueAndCommit(vid.str(), agentRef, JobQueueType::JobsToTransferForUser);
    cta::objectstore::RetrieveQueue rq(tAddr[i], be);
  }
  // Create the various ATFR's, stopping one step further each time.
  int pass = 0;
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
    rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
    rqc.archiveFile.creationTime = 0;
    rqc.archiveFile.reconciliationTime = 0;
    rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    rqc.archiveFile.diskInstance = "eoseos";
    rqc.archiveFile.fileSize = 1000 + pass;
    rqc.archiveFile.storageClass = "sc";
    {
      cta::common::dataStructures::TapeFile tf;
      tf.blockId=0;
      tf.fileSize=1;
      tf.copyNb=1;
      tf.creationTime=time(nullptr);
      tf.fSeq=pass;
      tf.vid="Tape0";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    {
      cta::common::dataStructures::TapeFile tf;
      tf.blockId=0;
      tf.fileSize=1;
      tf.copyNb=2;
      tf.creationTime=time(nullptr);
      tf.fSeq=pass;
      tf.vid="Tape1";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time=time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1, 1);
    rr.addJob(2, 1, 1, 1);
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
      jta.push_back({1,rqc.archiveFile.tapeFiles.front().fSeq, rr.getAddressIfSet(), rqc.archiveFile.fileSize, rqc.mountPolicy,
          sReq.creationLog.time, cta::nullopt, cta::nullopt});
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
    ASSERT_EQ(5, rq.getJobsSummary().jobs);
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
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    std::list<std::string> jtrl;
    for (auto &j: rq.dumpJobs()) {
      jtrl.push_back(j.address);
    }
    rq.removeJobsAndCommit(jtrl);
    rql.release();
    // Remove queues from root
    re.removeRetrieveQueueAndCommit(vid, JobQueueType::JobsToTransferForUser, lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(lc));
  ASSERT_NO_THROW(re.removeIfEmpty(lc));
  // TODO: this unit test still leaks tape pools and requests
}

TEST(ObjectStore, GarbageCollectorRepackRequestPending) {
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
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(lc);
    //Create a RepackQueue and insert a RepackRequest with status "Pending" in it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();

    //Create the RepackRequest
    std::string repackRequestAddr = agentReferenceRepackRequest.nextId("RepackRequest");
    agentReferenceRepackRequest.addToOwnership(repackRequestAddr, be);
    cta::objectstore::RepackRequest repackRequest(repackRequestAddr,be);
    repackRequest.initialize();
    repackRequest.setStatus(cta::common::dataStructures::RepackInfo::Status::Pending);
    repackRequest.setVid("VIDTest");
    repackRequest.setBufferURL("test/buffer/url");
    repackRequest.setOwner(agentReferenceRepackRequest.getAgentAddress());
    repackRequest.setMountPolicy(cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
    repackRequest.setCreationLog(cta::common::dataStructures::EntryLog("test","test",time(nullptr)));
    repackRequest.insert();
  }
  {
    //Now we garbage collect the RepackRequest

    // Create the garbage collector and run it once.
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
      gc.runOnePass(lc);
    }
  }
  //The repack request should have been requeued in the RepackQueuePending
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(agentRef,cta::common::dataStructures::RepackQueueType::Pending);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST(ObjectStore, GarbageCollectorRepackRequestToExpand) {
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
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(lc);
    //Create a RepackQueue and insert a RepackRequest with status "ToExpand" in it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();

    //Create the RepackRequest
    std::string repackRequestAddr = agentReferenceRepackRequest.nextId("RepackRequest");
    agentReferenceRepackRequest.addToOwnership(repackRequestAddr, be);
    cta::objectstore::RepackRequest repackRequest(repackRequestAddr,be);
    repackRequest.initialize();
    repackRequest.setStatus(cta::common::dataStructures::RepackInfo::Status::ToExpand);
    repackRequest.setVid("VID2Test");
    repackRequest.setBufferURL("test/buffer/url");
    repackRequest.setOwner(agentReferenceRepackRequest.getAgentAddress());
    repackRequest.setMountPolicy(cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
    repackRequest.setCreationLog(cta::common::dataStructures::EntryLog("test","test",time(nullptr)));
    repackRequest.insert();
  }
  {
    // Now we garbage collect the RepackRequest

    // Create the garbage collector and run it once.
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
      gc.runOnePass(lc);
    }
  }
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(agentRef,cta::common::dataStructures::RepackQueueType::ToExpand);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST(ObjectStore, GarbageCollectorRepackRequestRunningExpandNotFinished) {
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
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(lc);
    //Create a RepackQueue and insert a RepackRequest with status "ToExpand" in it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();

    //Create the RepackRequest
    std::string repackRequestAddr = agentReferenceRepackRequest.nextId("RepackRequest");
    agentReferenceRepackRequest.addToOwnership(repackRequestAddr, be);
    cta::objectstore::RepackRequest repackRequest(repackRequestAddr,be);
    repackRequest.initialize();
    repackRequest.setStatus(cta::common::dataStructures::RepackInfo::Status::Running);
    repackRequest.setVid("VIDTest");
    repackRequest.setBufferURL("test/buffer/url");
    repackRequest.setOwner(agentReferenceRepackRequest.getAgentAddress());
    repackRequest.setExpandFinished(false);
    repackRequest.setMountPolicy(cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
    repackRequest.setCreationLog(cta::common::dataStructures::EntryLog("test","test",time(nullptr)));
    repackRequest.insert();
  }
  {
    // Now we garbage collect the RepackRequest

    // Create the garbage collector and run it once.
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
      gc.runOnePass(lc);
    }
  }
  {
    //The request should be requeued in the ToExpand as it has not finished to expand
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(agentRef,cta::common::dataStructures::RepackQueueType::ToExpand);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST(ObjectStore, GarbageCollectorRepackRequestRunningExpandFinished) {
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
  std::string repackRequestAddress;
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(lc);
    //Create a RepackQueue and insert a RepackRequest with status "ToExpand" in it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();

    //Create the RepackRequest
    repackRequestAddress = agentReferenceRepackRequest.nextId("RepackRequest");
    agentReferenceRepackRequest.addToOwnership(repackRequestAddress, be);
    cta::objectstore::RepackRequest repackRequest(repackRequestAddress,be);
    repackRequest.initialize();
    repackRequest.setStatus(cta::common::dataStructures::RepackInfo::Status::Running);
    repackRequest.setVid("VIDTest");
    repackRequest.setBufferURL("test/buffer/url");
    repackRequest.setOwner(agentReferenceRepackRequest.getAgentAddress());
    repackRequest.setExpandFinished(true);
    repackRequest.setMountPolicy(cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
    repackRequest.setCreationLog(cta::common::dataStructures::EntryLog("test","test",time(nullptr)));
    repackRequest.insert();
  }
  cta::log::StringLogger strLogger("dummy", "dummy", cta::log::DEBUG);
  cta::log::LogContext lc2(strLogger);
  std::string agentGarbageCollectingRepackRequestAddress;
  {
    // Now we garbage collect the RepackRequest

    // Create the garbage collector and run it once.
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", strLogger);
    agentGarbageCollectingRepackRequestAddress = gcAgentRef.getAgentAddress();
    cta::objectstore::Agent gcAgent(agentGarbageCollectingRepackRequestAddress, be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(lc2);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
      gc.runOnePass(lc2);
    }
  }
  {
    //The request should not be requeued in the ToExpand queue as it has finished to expand and is running
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(agentRef,cta::common::dataStructures::RepackQueueType::ToExpand);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(0,rq.getRequestsSummary().requests);
  }
  {
    //The request should not be requeued in the Pending queue as it is running
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(agentRef,cta::common::dataStructures::RepackQueueType::Pending);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(0,rq.getRequestsSummary().requests);
  }
  {
    //Check that the owner of the repack request has been updated and is equal to the garbageCollector's agent
    cta::objectstore::RepackRequest repackRequest(repackRequestAddress,be);
    repackRequest.fetchNoLock();
    ASSERT_EQ(agentGarbageCollectingRepackRequestAddress,repackRequest.getOwner());
  }
  //Check the logs contains the failed to requeue message
  std::string logToCheck = strLogger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos,logToCheck.find("MSG=\"In RepackRequest::garbageCollect(): failed to requeue the RepackRequest (leaving it as it is) : The status Running has no corresponding queue type.\""));
}

TEST(ObjectStore, GarbageCollectorRepackRequestStarting) {
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
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(lc);
    //Create a RepackQueue and insert a RepackRequest with status "ToExpand" in it
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();

    //Create the RepackRequest
    std::string repackRequestAddr = agentReferenceRepackRequest.nextId("RepackRequest");
    agentReferenceRepackRequest.addToOwnership(repackRequestAddr, be);
    cta::objectstore::RepackRequest repackRequest(repackRequestAddr,be);
    repackRequest.initialize();
    repackRequest.setStatus(cta::common::dataStructures::RepackInfo::Status::Starting);
    repackRequest.setVid("VIDTest");
    repackRequest.setBufferURL("test/buffer/url");
    repackRequest.setOwner(agentReferenceRepackRequest.getAgentAddress());
    repackRequest.setExpandFinished(true);
    repackRequest.setMountPolicy(cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
    repackRequest.setCreationLog(cta::common::dataStructures::EntryLog("test","test",time(nullptr)));
    repackRequest.insert();
  }
  cta::log::StringLogger strLogger("dummy", "dummy", cta::log::DEBUG);
  cta::log::LogContext lc2(strLogger);
  {
    // Now we garbage collect the RepackRequest

    // Create the garbage collector and run it once.
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", strLogger);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(lc2);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
      gc.runOnePass(lc2);
    }
  }
  //Check the logs contains the failed to requeue message
  std::string logToCheck = strLogger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos,logToCheck.find("MSG=\"In RepackRequest::garbageCollect(): failed to requeue the RepackRequest (leaving it as it is) : The status Starting has no corresponding queue type.\""));
}

TEST(ObjectStore, GarbageCollectorRetrieveAllStatusesAndQueues) {
  using cta::common::dataStructures::JobQueueType;
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
  agent.setTimeout_us(100000000);
  agent.insertAndRegisterSelf(lc);
  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(lc);

  std::string retrieveRequestAddress = agentRefToTransferForUser.nextId("RetrieveRequest");
  agentRefToTransferForUser.addToOwnership(retrieveRequestAddress, be);

  cta::objectstore::RetrieveRequest rr(retrieveRequestAddress, be);

  rr.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
  rqc.archiveFile.archiveFileID = 123456789L;
  rqc.archiveFile.diskFileId = "eos://diskFile";
  rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=2;
    tf.creationTime=time(nullptr);
    tf.fSeq=1;
    tf.vid="Tape0";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);
  cta::common::dataStructures::RetrieveRequest sReq;
  sReq.archiveFileID = rqc.archiveFile.archiveFileID;
  sReq.creationLog.time=time(nullptr);
  rr.setSchedulerRequest(sReq);
  rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToTransfer);
  rr.setOwner(agentToTransferForUser.getAddressIfSet());
  rr.setActiveCopyNumber(0);
  rr.insert();

  // Create the garbage collector and run it once.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
  gc.runOnePass(lc);

  {
    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    auto jobs = rq.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rr.getOwner(),rq.getAddressIfSet());
  }

  {
    //Test the RetrieveRequest::garbageCollect method for RetrieveQueueToTransferForUser
    cta::objectstore::AgentReference agentRefToTransferForUserAutoGc("ToTransferForUser", dl);
    cta::objectstore::Agent agentToTransferForUserAutoGc(agentRefToTransferForUserAutoGc.getAgentAddress(), be);
    agentToTransferForUserAutoGc.initialize();
    agentToTransferForUserAutoGc.setTimeout_us(0);
    agentToTransferForUserAutoGc.insertAndRegisterSelf(lc);

    cta::objectstore::ScopedExclusiveLock sel(rr);
    rr.fetch();
    rr.setOwner(agentRefToTransferForUserAutoGc.getAgentAddress());
    agentRefToTransferForUserAutoGc.addToOwnership(rr.getAddressIfSet(),be);

    ASSERT_NO_THROW(rr.garbageCollect(agentRefToTransferForUserAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    sel.release();
    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    auto jobs = rq.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rr.getOwner(),rq.getAddressIfSet());
  }

  {
    //Test the Garbage collection of the RetrieveRequest with a reportToUserForFailure job
    cta::objectstore::AgentReference agentRefToReportToUser("ToReportToUser", dl);
    cta::objectstore::Agent agentToReportToUser(agentRefToReportToUser.getAgentAddress(), be);
    agentToReportToUser.initialize();
    agentToReportToUser.setTimeout_us(0);
    agentToReportToUser.insertAndRegisterSelf(lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToUser.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure);
      rr.commit();
    }

    agentRefToReportToUser.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Retrieve Request should be queued in the RetrieveQueueToReportToUser
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToUser(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    rqToReportToUser.fetchNoLock();

    auto jobs = rqToReportToUser.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);
  }

  {
    //Test the RetrieveRequest::garbageCollect method for ToReportToUserForFailure job
    cta::objectstore::AgentReference agentRefToReportToUserAutoGc("ToReportForUser", dl);
    cta::objectstore::Agent agentToReportToUserAutoGc(agentRefToReportToUserAutoGc.getAgentAddress(), be);
    agentToReportToUserAutoGc.initialize();
    agentToReportToUserAutoGc.setTimeout_us(0);
    agentToReportToUserAutoGc.insertAndRegisterSelf(lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToUserAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure);
      rr.commit();

      agentRefToReportToUserAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToUserAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    }

    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser

    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToUser(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    rqToReportToUser.fetchNoLock();

    auto jobs = rqToReportToUser.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rqToReportToUser.getAddressIfSet(),rr.getOwner());
  }

  {
    //Test the Garbage collection of the RetrieveRequest with a RJS_Failed job
    cta::objectstore::AgentReference agentRefFailedJob("FailedJob", dl);
    cta::objectstore::Agent agentFailedJob(agentRefFailedJob.getAgentAddress(), be);
    agentFailedJob.initialize();
    agentFailedJob.setTimeout_us(0);
    agentFailedJob.insertAndRegisterSelf(lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefFailedJob.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);
      rr.commit();
    }
    agentRefFailedJob.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Retrieve Request should be queued in the RetrieveQueueFailed
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqFailed(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    rqFailed.fetchNoLock();

    auto jobs = rqFailed.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);
  }

  {
    //Test the RetrieveRequest::garbageCollect method for RJS_Failed job
    cta::objectstore::AgentReference agentRefFailedJobAutoGc("FailedJob", dl);
    cta::objectstore::Agent agentFailedJobAutoGc(agentRefFailedJobAutoGc.getAgentAddress(), be);
    agentFailedJobAutoGc.initialize();
    agentFailedJobAutoGc.setTimeout_us(0);
    agentFailedJobAutoGc.insertAndRegisterSelf(lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefFailedJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);
      rr.commit();


      agentRefFailedJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefFailedJobAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    }

    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser

    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToUser(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    rqToReportToUser.fetchNoLock();

    auto jobs = rqToReportToUser.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rqToReportToUser.getAddressIfSet(),rr.getOwner());
  }

  //Create a repack info object for the garbage collection of Jobs ToReportToRepackForSuccess and ToReportToRepackForFailure
  cta::objectstore::RetrieveRequest::RepackInfo ri;
  ri.isRepack = true;
  ri.fSeq = 1;
  ri.fileBufferURL = "testFileBufferURL";
  ri.repackRequestAddress = "repackRequestAddress";

  {
    //Test the Garbage collection of the RetrieveRequest with a Retrieve job ToReportToRepackForSuccess
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccess("ToReportToRepackForSuccess", dl);
    cta::objectstore::Agent agentToReportToRepackForSuccess(agentRefToReportToRepackForSuccess.getAgentAddress(), be);
    agentToReportToRepackForSuccess.initialize();
    agentToReportToRepackForSuccess.setTimeout_us(0);
    agentToReportToRepackForSuccess.insertAndRegisterSelf(lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToRepackForSuccess.getAgentAddress());
      //Add the repack informations to the RetrieveRequest
      rr.setRepackInfo(ri);
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
      rr.commit();
    }
    agentRefToReportToRepackForSuccess.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Retrieve Request should be queued in the RetrieveQueueToReportToRepackForSuccess
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToRepackForSuccess(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    rqToReportToRepackForSuccess.fetchNoLock();

    auto jobs = rqToReportToRepackForSuccess.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);
  }

  {
    //Test the RetrieveRequest::garbageCollect method for RJS_ToReportToRepackForSuccess job
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccessJobAutoGc("ToReportToRepackForSuccessAutoGC", dl);
    cta::objectstore::Agent agentToReportToRepackForSuccessJobAutoGc(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress(), be);
    agentToReportToRepackForSuccessJobAutoGc.initialize();
    agentToReportToRepackForSuccessJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForSuccessJobAutoGc.insertAndRegisterSelf(lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
      rr.commit();

      agentRefToReportToRepackForSuccessJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    }

    //The Retrieve Request should now be queued in the RetrieveQueueToReportToRepackForSuccess

    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToRepackForSuccess(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    rqToReportToRepackForSuccess.fetchNoLock();

    auto jobs = rqToReportToRepackForSuccess.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rqToReportToRepackForSuccess.getAddressIfSet(),rr.getOwner());
  }

  {
    //Test the Garbage collection of the RetrieveRequest with a Retrieve job ToReportToRepackForFailure
    cta::objectstore::AgentReference agentRefToReportToRepackForFailure("ToReportToRepackForFailure", dl);
    cta::objectstore::Agent agentToReportToRepackForFailure(agentRefToReportToRepackForFailure.getAgentAddress(), be);
    agentToReportToRepackForFailure.initialize();
    agentToReportToRepackForFailure.setTimeout_us(0);
    agentToReportToRepackForFailure.insertAndRegisterSelf(lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    cta::objectstore::ScopedExclusiveLock sel(rr);
    rr.fetch();
    rr.setOwner(agentRefToReportToRepackForFailure.getAgentAddress());

    rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
    rr.commit();
    sel.release();

    agentRefToReportToRepackForFailure.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Retrieve Request should be queued in the RetrieveQueueToReportToRepackForFailure
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToRepackForFailure(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    rqToReportToRepackForFailure.fetchNoLock();

    auto jobs = rqToReportToRepackForFailure.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);
  }

  {
    //Test the RetrieveRequest::garbageCollect method for RJS_ToReportToRepackForSuccess job
    cta::objectstore::AgentReference agentRefToReportToRepackForFailureJobAutoGc("ToReportToRepackForFailureAutoGC", dl);
    cta::objectstore::Agent agentToReportToRepackForFailureJobAutoGc(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress(), be);
    agentToReportToRepackForFailureJobAutoGc.initialize();
    agentToReportToRepackForFailureJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForFailureJobAutoGc.insertAndRegisterSelf(lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
      rr.commit();

      agentRefToReportToRepackForFailureJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    }

    //The Retrieve Request should now be queued in the RetrieveQueueToReportToRepackForFailure

    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToReportToRepackForFailure(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    rqToReportToRepackForFailure.fetchNoLock();

    auto jobs = rqToReportToRepackForFailure.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rqToReportToRepackForFailure.getAddressIfSet(),rr.getOwner());
  }
}

TEST(ObjectStore, GarbageCollectorRetrieveRequestRepackDisabledTape) {
  using cta::common::dataStructures::JobQueueType;
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
  agent.setTimeout_us(10000);
  agent.insertAndRegisterSelf(lc);
  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(lc);

  std::string retrieveRequestAddress = agentRefToTransferForUser.nextId("RetrieveRequest");
  agentRefToTransferForUser.addToOwnership(retrieveRequestAddress, be);

  cta::objectstore::RetrieveRequest rr(retrieveRequestAddress, be);

  rr.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
  rqc.archiveFile.archiveFileID = 123456789L;
  rqc.archiveFile.diskFileId = "eos://diskFile";
  rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=2;
    tf.creationTime=time(nullptr);
    tf.fSeq=1;
    tf.vid="Tape0";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);
  cta::common::dataStructures::RetrieveRequest sReq;
  sReq.archiveFileID = rqc.archiveFile.archiveFileID;
  sReq.creationLog.time=time(nullptr);
  rr.setSchedulerRequest(sReq);
  rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToTransfer);
  rr.setOwner(agentToTransferForUser.getAddressIfSet());
  rr.setActiveCopyNumber(0);

  cta::objectstore::RetrieveRequest::RepackInfo ri;
  ri.isRepack = true;
  ri.forceDisabledTape = true;
  ri.fSeq = 1;
  ri.fileBufferURL = "testFileBufferURL";
  ri.repackRequestAddress = "repackRequestAddress";
  rr.setRepackInfo(ri);

  rr.insert();

  // Create the garbage collector and run it once.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);

  catalogue.addDisabledTape("Tape0");

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
  gc.runOnePass(lc);

  {
    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser
    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    auto jobs = rq.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rr.getOwner(),rq.getAddressIfSet());
  }

  {
    //Test the RetrieveRequest::garbageCollect method for RJS_ToTransferForUser job and a disabled tape
    cta::objectstore::AgentReference agentRefToTransferDisabledTapeAutoGc("ToReportToRepackForFailureAutoGC", dl);
    cta::objectstore::Agent agentToReportToRepackForFailureJobAutoGc(agentRefToTransferDisabledTapeAutoGc.getAgentAddress(), be);
    agentToReportToRepackForFailureJobAutoGc.initialize();
    agentToReportToRepackForFailureJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForFailureJobAutoGc.insertAndRegisterSelf(lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()});
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToTransferDisabledTapeAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToTransfer);
      rr.commit();

      agentRefToTransferDisabledTapeAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToTransferDisabledTapeAutoGc.getAgentAddress(),agentRef,lc,catalogue));
    }

    //The Retrieve Request should now be queued in the RetrieveQueueToTransferForUser

    re.fetchNoLock();
    cta::objectstore::RetrieveQueue rqToTransferForUser(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    rqToTransferForUser.fetchNoLock();

    auto jobs = rqToTransferForUser.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    rr.fetchNoLock();
    ASSERT_EQ(rqToTransferForUser.getAddressIfSet(),rr.getOwner());
  }
}

TEST(ObjectStore, GarbageCollectorArchiveAllStatusesAndQueues) {
  using cta::common::dataStructures::JobQueueType;
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

  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(lc);

  std::string archiveRequestAddress = agentRefToTransferForUser.nextId("ArchiveRequest");
  agentRefToTransferForUser.addToOwnership(archiveRequestAddress, be);

  std::string tapePool = "tapePool";

  cta::objectstore::ArchiveRequest ar(archiveRequestAddress, be);
  ar.initialize();
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = 123456789L;
  aFile.diskFileId = "eos://diskFile";
  aFile.checksumBlob.insert(cta::checksum::NONE, "");
  aFile.creationTime = 0;
  aFile.reconciliationTime = 0;
  aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  aFile.diskInstance = "eoseos";
  aFile.fileSize = 667;
  aFile.storageClass = "sc";
  ar.setArchiveFile(aFile);
  ar.addJob(2, tapePool, agentRefToTransferForUser.getAgentAddress(), 1, 1, 1);
  cta::common::dataStructures::MountPolicy mp;
  ar.setMountPolicy(mp);
  ar.setArchiveReportURL("");
  ar.setArchiveErrorReportURL("");
  ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
  ar.setSrcURL("root://eoseos/myFile");
  ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar.insert();

  // Create the garbage collector and run it once.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(lc);

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, catalogue);
  gc.runOnePass(lc);

  {
    //The Archive Request should now be queued in the ArchiveQueueToTransferForUser
    re.fetchNoLock();
    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    auto jobs = aq.dumpJobs();
    ASSERT_EQ(1,jobs.size());

    auto& job = jobs.front();
    ASSERT_EQ(2,job.copyNb);

    ar.fetchNoLock();
    ASSERT_EQ(ar.getJobOwner(2),aq.getAddressIfSet());
  }
  {
    //Test the AJS_ToTransferForUser auto garbage collection
    cta::objectstore::AgentReference agentRefToTransferForUserAutoGC("ToTransferForUserAutoGC", dl);
    cta::objectstore::Agent agentToTransferForUserAutoGC(agentRefToTransferForUserAutoGC.getAgentAddress(), be);
    agentToTransferForUserAutoGC.initialize();
    agentToTransferForUserAutoGC.setTimeout_us(0);
    agentToTransferForUserAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToTransferForUserAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToTransferForUser);
    ar.commit();
    agentRefToTransferForUserAutoGC.addToOwnership(ar.getAddressIfSet(),be);

    ar.garbageCollect(agentRefToTransferForUserAutoGC.getAgentAddress(),agentRef,lc,catalogue);
    sel.release();

    {
      //The Archive Request should be queued in the ArchiveQueueToTransferForUser
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToTransferForUser(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);

      aqToTransferForUser.fetchNoLock();

      auto jobs = aqToTransferForUser.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToTransferForUser.getAddressIfSet());
    }
  }
  {
    //Test the AJS_ToReportToUserForFailure Garbage collection
    cta::objectstore::AgentReference agentRefToReportToUserForFailure("ToReportToUserForFailure", dl);
    cta::objectstore::Agent agentToReportToUserForFailure(agentRefToReportToUserForFailure.getAgentAddress(), be);
    agentToReportToUserForFailure.initialize();
    agentToReportToUserForFailure.setTimeout_us(0);
    agentToReportToUserForFailure.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForFailure.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForFailure);
    ar.commit();
    sel.release();

    agentRefToReportToUserForFailure.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Archive Request should be queued in the ArchiveQueueToReportForUser
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToUserForFailure(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);

      aqToReportToUserForFailure.fetchNoLock();

      auto jobs = aqToReportToUserForFailure.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToUserForFailure.getAddressIfSet());
    }
  }
  {
    //Test the AJS_ToReportToUserForFailure Auto Garbage collection
    cta::objectstore::AgentReference agentRefToReportToUserForFailureAutoGC("ToReportToUserForFailureAutoGC", dl);
    cta::objectstore::Agent agentToReportToUserForFailureAutoGC(agentRefToReportToUserForFailureAutoGC.getAgentAddress(), be);
    agentToReportToUserForFailureAutoGC.initialize();
    agentToReportToUserForFailureAutoGC.setTimeout_us(0);
    agentToReportToUserForFailureAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForFailureAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForFailure);
    ar.commit();
    agentRefToReportToUserForFailureAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToUserForFailureAutoGC.getAgentAddress(),agentRef,lc,catalogue);

    //The Archive Request should be queued in the ArchiveQueueToReportForUser
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToUserForFailure(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);

      aqToReportToUserForFailure.fetchNoLock();

      auto jobs = aqToReportToUserForFailure.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToUserForFailure.getAddressIfSet());
    }
  }

  {
    //Test the AJS_ToReportToUserForTransfer Garbage collection
    cta::objectstore::AgentReference agentRefToReportToUserForTransfer("ToReportToUserForTransfer", dl);
    cta::objectstore::Agent agentToReportToUserForTransfer(agentRefToReportToUserForTransfer.getAgentAddress(), be);
    agentToReportToUserForTransfer.initialize();
    agentToReportToUserForTransfer.setTimeout_us(0);
    agentToReportToUserForTransfer.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForTransfer.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
    ar.commit();
    sel.release();

    agentRefToReportToUserForTransfer.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Archive Request should be queued in the ArchiveQueueToReportForUser
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToUserForTransfer(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);

      aqToReportToUserForTransfer.fetchNoLock();

      auto jobs = aqToReportToUserForTransfer.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToUserForTransfer.getAddressIfSet());
    }
  }
  {
    //Test the AJS_ToReportToUserForTransfer Auto Garbage collection
    cta::objectstore::AgentReference agentRefToReportToUserForTransferAutoGC("ToReportToUserForTransferAutoGC", dl);
    cta::objectstore::Agent agentToReportToUserForTransferAutoGC(agentRefToReportToUserForTransferAutoGC.getAgentAddress(), be);
    agentToReportToUserForTransferAutoGC.initialize();
    agentToReportToUserForTransferAutoGC.setTimeout_us(0);
    agentToReportToUserForTransferAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForTransferAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
    ar.commit();
    agentRefToReportToUserForTransferAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToUserForTransferAutoGC.getAgentAddress(),agentRef,lc,catalogue);

    //The Archive Request should be queued in the ArchiveQueueToReportForUser
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToUserForTransfer(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);

      aqToReportToUserForTransfer.fetchNoLock();

      auto jobs = aqToReportToUserForTransfer.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToUserForTransfer.getAddressIfSet());
    }
  }
  {
    //Test the garbage collection of an AJS_Failed job
    cta::objectstore::AgentReference agentRefFailed("Failed", dl);
    cta::objectstore::Agent agentFailed(agentRefFailed.getAgentAddress(), be);
    agentFailed.initialize();
    agentFailed.setTimeout_us(0);
    agentFailed.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefFailed.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_Failed);
    ar.commit();
    sel.release();

    agentRefFailed.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Archive Request should be queued in the ArchiveQueueFailed
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqFailed(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);

      aqFailed.fetchNoLock();

      auto jobs = aqFailed.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqFailed.getAddressIfSet());
    }
  }

  {
    //Test the AJS_Failed job Auto Garbage collection
    cta::objectstore::AgentReference agentRefFailedAutoGC("FailedAutoGC", dl);
    cta::objectstore::Agent agentFailedAutoGC(agentRefFailedAutoGC.getAgentAddress(), be);
    agentFailedAutoGC.initialize();
    agentFailedAutoGC.setTimeout_us(0);
    agentFailedAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefFailedAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_Failed);
    ar.commit();
    agentRefFailedAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefFailedAutoGC.getAgentAddress(),agentRef,lc,catalogue);

    //The Archive Request should be queued in the ArchiveQueueFailed
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqFailed(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);

      aqFailed.fetchNoLock();

      auto jobs = aqFailed.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqFailed.getAddressIfSet());
    }
  }

  //Add Repack informations to test the garbage collection of Archive Requests for Repack
  //Create a repack info object for the garbage collection of Jobs ToReportToRepackForSuccess and ToReportToRepackForFailure
  cta::objectstore::ArchiveRequest::RepackInfo ri;
  ri.isRepack = true;
  ri.fSeq = 1;
  ri.fileBufferURL = "testFileBufferURL";
  ri.repackRequestAddress = "repackRequestAddress";

  {
    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setRepackInfo(ri);
    ar.commit();
  }

  {
    //Test the Garbage collection of an AJS_ToReportToRepackForSuccess job
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccess("ToReportToUserForTransfer", dl);
    cta::objectstore::Agent agentToReportToRepackForSuccess(agentRefToReportToRepackForSuccess.getAgentAddress(), be);
    agentToReportToRepackForSuccess.initialize();
    agentToReportToRepackForSuccess.setTimeout_us(0);
    agentToReportToRepackForSuccess.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForSuccess.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForSuccess);
    ar.commit();
    sel.release();

    agentRefToReportToRepackForSuccess.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Archive Request should be queued in the ArchiveQueueToReportToRepackForSuccess
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToRepackForSuccess(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);

      aqToReportToRepackForSuccess.fetchNoLock();

      auto jobs = aqToReportToRepackForSuccess.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToRepackForSuccess.getAddressIfSet());
    }
  }

  {
    //Test the AJS_ToReportToRepackForSuccess job Auto Garbage collection
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccessAutoGC("ToReportToRepackForSuccessAutoGC", dl);
    cta::objectstore::Agent agentToReportToRepackForSuccessAutoGC(agentRefToReportToRepackForSuccessAutoGC.getAgentAddress(), be);
    agentToReportToRepackForSuccessAutoGC.initialize();
    agentToReportToRepackForSuccessAutoGC.setTimeout_us(0);
    agentToReportToRepackForSuccessAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForSuccessAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForSuccess);
    ar.commit();
    agentRefToReportToRepackForSuccessAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToRepackForSuccessAutoGC.getAgentAddress(),agentRef,lc,catalogue);

    //The Archive Request should be queued in the ArchiveQueueToReportToRepackForSuccess
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToRepackForSuccess(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);

      aqToReportToRepackForSuccess.fetchNoLock();

      auto jobs = aqToReportToRepackForSuccess.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToRepackForSuccess.getAddressIfSet());
    }
  }

  {
    //Test the garbage collection of an AJS_ToReportToRepackForFailure job
    cta::objectstore::AgentReference agentRefToReportToRepackForFailure("ToReportToRepackForFailure", dl);
    cta::objectstore::Agent agentToReportToRepackForFailure(agentRefToReportToRepackForFailure.getAgentAddress(), be);
    agentToReportToRepackForFailure.initialize();
    agentToReportToRepackForFailure.setTimeout_us(0);
    agentToReportToRepackForFailure.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForFailure.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForFailure);
    ar.commit();
    sel.release();

    agentRefToReportToRepackForFailure.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(lc);

    //The Archive Request should be queued in the ArchiveQueueToReportToRepackForFailure
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToRepackForFailure(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);

      aqToReportToRepackForFailure.fetchNoLock();

      auto jobs = aqToReportToRepackForFailure.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToRepackForFailure.getAddressIfSet());
    }
  }
  {
    //Test the AJS_ToReportToRepackForFailure job Auto Garbage collection
    cta::objectstore::AgentReference agentRefToReportToRepackForFailureAutoGC("ToReportToRepackForFailureAutoGC", dl);
    cta::objectstore::Agent agentToReportToRepackForFailureAutoGC(agentRefToReportToRepackForFailureAutoGC.getAgentAddress(), be);
    agentToReportToRepackForFailureAutoGC.initialize();
    agentToReportToRepackForFailureAutoGC.setTimeout_us(0);
    agentToReportToRepackForFailureAutoGC.insertAndRegisterSelf(lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()});
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForFailureAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForFailure);
    ar.commit();
    agentRefToReportToRepackForFailureAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToRepackForFailureAutoGC.getAgentAddress(),agentRef,lc,catalogue);

    //The Archive Request should be queued in the ArchiveQueueToReportToRepackForFailure
    {
      re.fetchNoLock();
      cta::objectstore::ArchiveQueue aqToReportToRepackForFailure(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);

      aqToReportToRepackForFailure.fetchNoLock();

      auto jobs = aqToReportToRepackForFailure.dumpJobs();
      ASSERT_EQ(1,jobs.size());

      auto& job = jobs.front();
      ASSERT_EQ(2,job.copyNb);

      ar.fetchNoLock();
      ASSERT_EQ(ar.getJobOwner(2),aqToReportToRepackForFailure.getAddressIfSet());
    }
  }
}

}
