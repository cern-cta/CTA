
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
#include <memory>

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "ArchiveQueue.hpp"
#include "ArchiveRequest.hpp"
#include "BackendVFS.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/RetrieveJobToAdd.hpp"
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

#include "ObjectStoreFixture.hpp"

namespace unitTests {

using cta::common::dataStructures::JobQueueType;

class GarbageCollectorTest: public ::testing::Test {
public:

  //GarbageCollectorTest() : m_catalogue(), m_dl({"dummy", "unitTest"}), m_lc(m_dl), be(), re(be), m_agentRef("unitTestCreateEnv", m_dl) {};
  GarbageCollectorTest() : m_dl("dummy", "unitTest"), m_lc(m_dl) {};

  class FailedToGetBackend : public std::exception {
  public:
      const char *what() const noexcept override {
        return "failedto get VFS backend";
      }
  };

  class FailedToGetCatalogue : public std::exception {
  public:
      const char *what() const noexcept override {
        return "failedto get catalogue";
      }
  };

  class FailedToGetRootEntry : public std::exception {
  public:
      const char *what() const noexcept override {
        return "failedto get root entry";
      }
  };

  class FailedToGetAgentRef : public std::exception {
  public:
      const char *what() const noexcept override {
        return "failedto get agentref";
      }
  };

  virtual void SetUp() {
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    m_be = std::make_unique<cta::objectstore::BackendVFS>();
    m_re = std::make_unique<cta::objectstore::RootEntry>(getBackend());
    m_re->initialize();
    m_re->insert();
    // Create the agent register
    m_agentRef = std::make_unique<cta::objectstore::AgentReference>("unitTestCreateEnv", m_dl);

    cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
    cta::objectstore::ScopedExclusiveLock rel(getRootEntry());
    // Finish root creation.
    m_re->addOrGetAgentRegisterPointerAndCommit(getAgentRef(), el, m_lc);
  }

  virtual void TearDown() {
    cta::objectstore::Helpers::flushStatisticsCache();
    m_catalogue.reset();
    m_be.reset();
    m_re.reset();
    m_agentRef.reset();
  }

  cta::catalogue::DummyCatalogue& getCatalogue() {
    cta::catalogue::DummyCatalogue *const ptr = m_catalogue.get();
    if(nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::objectstore::BackendVFS& getBackend() {
    cta::objectstore::BackendVFS *const ptr = m_be.get();
    if(nullptr == ptr) {
      throw FailedToGetBackend();
    }
    return *ptr;
  }

  cta::objectstore::RootEntry& getRootEntry() {
    cta::objectstore::RootEntry *const ptr = m_re.get();
    if(nullptr == ptr) {
      throw FailedToGetRootEntry();
    }
    return *ptr;
  }

  cta::objectstore::AgentReference& getAgentRef() {
    cta::objectstore::AgentReference *const ptr = m_agentRef.get();
    if(nullptr == ptr) {
      throw FailedToGetAgentRef();
    }
    return *ptr;
  }

  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger m_dl;
#else
  cta::log::DummyLogger m_dl;
#endif
  
  cta::log::LogContext m_lc;

private:
  // Prevent copying
  GarbageCollectorTest(const GarbageCollectorTest &) = delete;

  // Prevent assignment
  GarbageCollectorTest & operator= (const GarbageCollectorTest &) = delete;

  std::unique_ptr<cta::catalogue::DummyCatalogue> m_catalogue;
  std::unique_ptr<cta::objectstore::BackendVFS> m_be;
  std::unique_ptr<cta::objectstore::RootEntry> m_re;
  std::unique_ptr<cta::objectstore::AgentReference> m_agentRef;

};

TEST_F(GarbageCollectorTest, GarbageCollectorBasicFuctionnality) {
  auto re = getRootEntry();
  auto be = getBackend();
  // Create 2 agents, A and B and register them
  // The agents are set with a timeout of 0, so they will be declared
  // dead immediately.
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl), agrB("unitTestAgentB", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be), agB(agrB.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
  agB.initialize();
  agB.setTimeout_us(0);
  agB.insertAndRegisterSelf(m_lc);
  // Create the garbage colletor and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
  }
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
}

TEST_F(GarbageCollectorTest, GarbageCollectorRegister) {
  auto re = getRootEntry();
  auto be = getBackend();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
  }
  ASSERT_FALSE(be.exists(arName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
}

TEST_F(GarbageCollectorTest, GarbageCollectorArchiveQueue) {
  auto re = getRootEntry();
  auto be = getBackend();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
}

TEST_F(GarbageCollectorTest, GarbageCollectorDriveRegister) {
 auto re = getRootEntry();
  auto be = getBackend();
 // Create an agent and add the drive register to it as an owned object
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
  }
  ASSERT_FALSE(be.exists(tpName));
  // Unregister gc's agent
  cta::objectstore::ScopedExclusiveLock gcal(gcAgent);
  gcAgent.fetch();
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
  rel.lock(re);
  re.fetch();
  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
}

TEST_F(GarbageCollectorTest, GarbageCollectorArchiveRequest) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  // Create an agent to garbage collected
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
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
    tpAddr[i] = re.addOrGetArchiveQueueAndCommit(tapePoolName.str(), getAgentRef(), JobQueueType::JobsToTransferForUser);
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
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000U+pass, policy, time(nullptr)});
      aq.addJobsAndCommit(jta, getAgentRef(), m_lc);
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
      jta.push_back({jd, ar.getAddressIfSet(), ar.getArchiveFile().archiveFileID, 1000+pass, policy, time(nullptr)});
      aq.addJobsAndCommit(jta, getAgentRef(), m_lc);
      ar.setJobOwner(2, aq.getAddressIfSet());
      ar.commit();
    }
    if (pass < 5) { pass++; continue; }
    // The step is now deprecated
    break;
  }
  // Create the garbage collector and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
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
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
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
    aq.removeJobsAndCommit(ajtr, m_lc);
    aql.release();
    // Remove queues from root
    re.removeArchiveQueueAndCommit(tp, JobQueueType::JobsToTransferForUser, m_lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
  // TODO: this unit test still leaks tape pools and requests
}

TEST_F(GarbageCollectorTest, GarbageCollectorRetrieveRequest) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  // Create an agent to garbage be collected
  cta::objectstore::AgentReference agrA("unitTestAgentA", m_dl);
  cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
  agA.initialize();
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf(m_lc);
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
    tAddr[i] = re.addOrGetRetrieveQueueAndCommit(vid.str(), getAgentRef(), JobQueueType::JobsToTransferForUser);
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
      std::list <cta::common::dataStructures::RetrieveJobToAdd> jta;
      jta.push_back({1,rqc.archiveFile.tapeFiles.front().fSeq, rr.getAddressIfSet(), rqc.archiveFile.fileSize, rqc.mountPolicy,
          sReq.creationLog.time, std::nullopt, std::nullopt});
      rq.addJobsAndCommit(jta, getAgentRef(), m_lc);
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
  static_cast<cta::catalogue::DummyTapeCatalogue*>(getCatalogue().Tape().get())->addEnabledTape("Tape0");
  // Mark the other tape as disabled
  static_cast<cta::catalogue::DummyTapeCatalogue*>(getCatalogue().Tape().get())->addDisabledTape("Tape1");
  // Create the garbage collector and run it twice.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);
  {
    cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
    gc.runOnePass(m_lc);
    gc.runOnePass(m_lc);
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
  gcAgent.removeAndUnregisterSelf(m_lc);
  // We should not be able to remove the agent register (as it should be empty)
  cta::objectstore::ScopedExclusiveLock rel;
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
    rq.removeJobsAndCommit(jtrl, m_lc);
    rql.release();
    // Remove queues from root
    re.removeRetrieveQueueAndCommit(vid, JobQueueType::JobsToTransferForUser, m_lc);
  }

  ASSERT_NO_THROW(re.removeAgentRegisterAndCommit(m_lc));
  ASSERT_NO_THROW(re.removeIfEmpty(m_lc));
  // TODO: this unit test still leaks tape pools and requests
}

TEST_F(GarbageCollectorTest, GarbageCollectorRepackRequestPending) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", m_dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(m_lc);
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
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(m_lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
      gc.runOnePass(m_lc);
    }
  }
  //The repack request should have been requeued in the RepackQueuePending
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(getAgentRef(),cta::common::dataStructures::RepackQueueType::Pending);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST_F(GarbageCollectorTest, GarbageCollectorRepackRequestToExpand) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", m_dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(m_lc);
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
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(m_lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
      gc.runOnePass(m_lc);
    }
  }
  {
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(getAgentRef(),cta::common::dataStructures::RepackQueueType::ToExpand);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST_F(GarbageCollectorTest, GarbageCollectorRepackRequestRunningExpandNotFinished) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", m_dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(m_lc);
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
    cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
    cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
    gcAgent.initialize();
    gcAgent.setTimeout_us(0);
    gcAgent.insertAndRegisterSelf(m_lc);
    {
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
      gc.runOnePass(m_lc);
    }
  }
  {
    //The request should be requeued in the ToExpand as it has not finished to expand
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(getAgentRef(),cta::common::dataStructures::RepackQueueType::ToExpand);
    cta::objectstore::RepackQueue rq(repackQueueAddr,be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    ASSERT_EQ(1,rq.getRequestsSummary().requests);
  }
}

TEST_F(GarbageCollectorTest, GarbageCollectorRepackRequestRunningExpandFinished) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  std::string repackRequestAddress;
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", m_dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(m_lc);
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
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
      gc.runOnePass(lc2);
    }
  }
  {
    //The request should not be requeued in the ToExpand queue as it has finished to expand and is running
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(getAgentRef(),cta::common::dataStructures::RepackQueueType::ToExpand);
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
    std::string repackQueueAddr = re.addOrGetRepackQueueAndCommit(getAgentRef(),cta::common::dataStructures::RepackQueueType::Pending);
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

TEST_F(GarbageCollectorTest, GarbageCollectorRepackRequestStarting) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);
  {
    // Create an agent to be garbage collected
    cta::objectstore::AgentReference agentReferenceRepackRequest("AgentReferenceRepackRequest", m_dl);
    cta::objectstore::Agent agentRepackRequest(agentReferenceRepackRequest.getAgentAddress(), be);
    agentRepackRequest.initialize();
    agentRepackRequest.setTimeout_us(0);
    agentRepackRequest.insertAndRegisterSelf(m_lc);
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
      cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
      gc.runOnePass(lc2);
    }
  }
  //Check the logs contains the failed to requeue message
  std::string logToCheck = strLogger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos,logToCheck.find("MSG=\"In RepackRequest::garbageCollect(): failed to requeue the RepackRequest (leaving it as it is) : The status Starting has no corresponding queue type.\""));
}

TEST_F(GarbageCollectorTest, GarbageCollectorRetrieveAllStatusesAndQueues) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(100000000);
  agent.insertAndRegisterSelf(m_lc);
  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", m_dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(m_lc);

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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
  gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToTransferForUserAutoGc("ToTransferForUser", m_dl);
    cta::objectstore::Agent agentToTransferForUserAutoGc(agentRefToTransferForUserAutoGc.getAgentAddress(), be);
    agentToTransferForUserAutoGc.initialize();
    agentToTransferForUserAutoGc.setTimeout_us(0);
    agentToTransferForUserAutoGc.insertAndRegisterSelf(m_lc);

    cta::objectstore::ScopedExclusiveLock sel(rr);
    rr.fetch();
    rr.setOwner(agentRefToTransferForUserAutoGc.getAgentAddress());
    agentRefToTransferForUserAutoGc.addToOwnership(rr.getAddressIfSet(),be);

    ASSERT_NO_THROW(rr.garbageCollect(agentRefToTransferForUserAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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
    cta::objectstore::AgentReference agentRefToReportToUser("ToReportToUser", m_dl);
    cta::objectstore::Agent agentToReportToUser(agentRefToReportToUser.getAgentAddress(), be);
    agentToReportToUser.initialize();
    agentToReportToUser.setTimeout_us(0);
    agentToReportToUser.insertAndRegisterSelf(m_lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToUser.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure);
      rr.commit();
    }

    agentRefToReportToUser.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToUserAutoGc("ToReportForUser", m_dl);
    cta::objectstore::Agent agentToReportToUserAutoGc(agentRefToReportToUserAutoGc.getAgentAddress(), be);
    agentToReportToUserAutoGc.initialize();
    agentToReportToUserAutoGc.setTimeout_us(0);
    agentToReportToUserAutoGc.insertAndRegisterSelf(m_lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToUserAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure);
      rr.commit();

      agentRefToReportToUserAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToUserAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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
    cta::objectstore::AgentReference agentRefFailedJob("FailedJob", m_dl);
    cta::objectstore::Agent agentFailedJob(agentRefFailedJob.getAgentAddress(), be);
    agentFailedJob.initialize();
    agentFailedJob.setTimeout_us(0);
    agentFailedJob.insertAndRegisterSelf(m_lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefFailedJob.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);
      rr.commit();
    }
    agentRefFailedJob.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefFailedJobAutoGc("FailedJob", m_dl);
    cta::objectstore::Agent agentFailedJobAutoGc(agentRefFailedJobAutoGc.getAgentAddress(), be);
    agentFailedJobAutoGc.initialize();
    agentFailedJobAutoGc.setTimeout_us(0);
    agentFailedJobAutoGc.insertAndRegisterSelf(m_lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefFailedJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);
      rr.commit();


      agentRefFailedJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefFailedJobAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccess("ToReportToRepackForSuccess", m_dl);
    cta::objectstore::Agent agentToReportToRepackForSuccess(agentRefToReportToRepackForSuccess.getAgentAddress(), be);
    agentToReportToRepackForSuccess.initialize();
    agentToReportToRepackForSuccess.setTimeout_us(0);
    agentToReportToRepackForSuccess.insertAndRegisterSelf(m_lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
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

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccessJobAutoGc("ToReportToRepackForSuccessAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToRepackForSuccessJobAutoGc(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress(), be);
    agentToReportToRepackForSuccessJobAutoGc.initialize();
    agentToReportToRepackForSuccessJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForSuccessJobAutoGc.insertAndRegisterSelf(m_lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
      rr.commit();

      agentRefToReportToRepackForSuccessJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToRepackForSuccessJobAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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
    cta::objectstore::AgentReference agentRefToReportToRepackForFailure("ToReportToRepackForFailure", m_dl);
    cta::objectstore::Agent agentToReportToRepackForFailure(agentRefToReportToRepackForFailure.getAgentAddress(), be);
    agentToReportToRepackForFailure.initialize();
    agentToReportToRepackForFailure.setTimeout_us(0);
    agentToReportToRepackForFailure.insertAndRegisterSelf(m_lc);

    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    cta::objectstore::ScopedExclusiveLock sel(rr);
    rr.fetch();
    rr.setOwner(agentRefToReportToRepackForFailure.getAgentAddress());

    rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
    rr.commit();
    sel.release();

    agentRefToReportToRepackForFailure.addToOwnership(rr.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToRepackForFailureJobAutoGc("ToReportToRepackForFailureAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToRepackForFailureJobAutoGc(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress(), be);
    agentToReportToRepackForFailureJobAutoGc.initialize();
    agentToReportToRepackForFailureJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForFailureJobAutoGc.insertAndRegisterSelf(m_lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
      rr.commit();

      agentRefToReportToRepackForFailureJobAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToReportToRepackForFailureJobAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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

TEST_F(GarbageCollectorTest, GarbageCollectorRetrieveRequestRepackRepackingTape) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(10000);
  agent.insertAndRegisterSelf(m_lc);
  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", m_dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(m_lc);

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
  ri.fSeq = 1;
  ri.fileBufferURL = "testFileBufferURL";
  ri.repackRequestAddress = "repackRequestAddress";
  rr.setRepackInfo(ri);

  rr.insert();

  // Create the garbage collector and run it once.
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);

  static_cast<cta::catalogue::DummyTapeCatalogue*>(getCatalogue().Tape().get())->addRepackingTape("Tape0");

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
  gc.runOnePass(m_lc);

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
    //Test the RetrieveRequest::garbageCollect method for RJS_ToTransferForUser job and a repacking tape
    cta::objectstore::AgentReference agentRefToTransferRepackingTapeAutoGc("ToReportToRepackForFailureAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToRepackForFailureJobAutoGc(agentRefToTransferRepackingTapeAutoGc.getAgentAddress(), be);
    agentToReportToRepackForFailureJobAutoGc.initialize();
    agentToReportToRepackForFailureJobAutoGc.setTimeout_us(0);
    agentToReportToRepackForFailureJobAutoGc.insertAndRegisterSelf(m_lc);


    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress("Tape0", JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.removeJobsAndCommit({rr.getAddressIfSet()}, m_lc);
    rql.release();

    {
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      rr.setOwner(agentRefToTransferRepackingTapeAutoGc.getAgentAddress());
      rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToTransfer);
      rr.commit();

      agentRefToTransferRepackingTapeAutoGc.addToOwnership(rr.getAddressIfSet(),be);

      ASSERT_NO_THROW(rr.garbageCollect(agentRefToTransferRepackingTapeAutoGc.getAgentAddress(),getAgentRef(),m_lc,getCatalogue()));
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


TEST_F(GarbageCollectorTest, GarbageCollectToReportRetrieveQueue) {
  auto re = getRootEntry();
  auto be = getBackend();
  // We only need to create the agent for the cleanup.
  // There is no need to create the ones that register for the cleanup info struct
  // we will populate that information manually.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);

  cta::objectstore::AgentReference agentRef2("custom", m_dl);
  cta::objectstore::Agent deadAgent(agentRef2.getAgentAddress(), be);
  deadAgent.initialize();
  deadAgent.setTimeout_us(0);
  deadAgent.insertAndRegisterSelf(m_lc);

  // Create a retrieve queue and populate the cleanup info.
  // This is basically a rperoduction of OStoreDB::reserveREtrieveQueueForCleanup
  const std::string tapeAddr = "Tape0";
  {
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    const auto queueAddr = re.addOrGetRetrieveQueueAndCommit(tapeAddr, getAgentRef(), JobQueueType::JobsToTransferForUser);
    cta::objectstore::RetrieveQueue rq(queueAddr, be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();
    rq.setOwner(agentRef2.getAgentAddress());
    rq.setQueueCleanupDoCleanup();
    rq.setQueueCleanupAssignedAgent(deadAgent.getAddressIfSet());
    rq.commit();
    // Create the ToReport queue and populate the cleanup info.
    const auto reportQueueName = re.addOrGetRetrieveQueueAndCommit(tapeAddr, getAgentRef(), JobQueueType::JobsToReportToUser);
    cta::objectstore::RetrieveQueue rqtr(reportQueueName, be);
    cta::objectstore::ScopedExclusiveLock rqtrl(rqtr);
    rqtr.fetch();
    rqtr.setOwner(agentRef2.getAgentAddress());
    rqtr.setQueueCleanupDoCleanup();
    rqtr.setQueueCleanupAssignedAgent(deadAgent.getAddressIfSet());
    rqtr.commit();
    
    // Check the Agent has been set. 
    ASSERT_EQ(rq.getQueueCleanupAssignedAgent().value(), agentRef2.getAgentAddress());
    ASSERT_EQ(rqtr.getQueueCleanupAssignedAgent().value(), agentRef2.getAgentAddress());

    // Add the queues to the object ownership.
    agentRef2.addToOwnership(rq.getAddressIfSet() ,be);
    agentRef2.addToOwnership(rqtr.getAddressIfSet(), be);
  }

  // We don't need to populate the queue with requests as the job of the
  // garbage collector is just to clear the CleanUp info
  // Run the garbage collector.
  cta::objectstore::GarbageCollector gc(be, getAgentRef(), getCatalogue());
  gc.runOnePass(m_lc);

  // The cleanup info should no be gone.
  {
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    cta::objectstore::RetrieveQueue rq(re.addOrGetRetrieveQueueAndCommit(tapeAddr, getAgentRef(), JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::RetrieveQueue rqtr(re.addOrGetRetrieveQueueAndCommit(tapeAddr, getAgentRef(), JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock rql(rq);
    cta::objectstore::ScopedExclusiveLock rqtrl(rqtr);
    rq.fetch();
    rqtr.fetch();
    ASSERT_EQ(rq.getQueueCleanupAssignedAgent().has_value(), false);
    ASSERT_EQ(rqtr.getQueueCleanupAssignedAgent().has_value(), false);
  }
}


TEST_F(GarbageCollectorTest, GarbageCollectorArchiveAllStatusesAndQueues) {
  auto re = getRootEntry();
  auto be = getBackend();
  // continue agent creation.
  cta::objectstore::Agent agent(getAgentRef().getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(m_lc);

  // Create all agents to be garbage collected
  cta::objectstore::AgentReference agentRefToTransferForUser("ToTransferForUser", m_dl);
  cta::objectstore::Agent agentToTransferForUser(agentRefToTransferForUser.getAgentAddress(), be);
  agentToTransferForUser.initialize();
  agentToTransferForUser.setTimeout_us(0);
  agentToTransferForUser.insertAndRegisterSelf(m_lc);

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
  cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", m_dl);
  cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), be);
  gcAgent.initialize();
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf(m_lc);

  cta::objectstore::GarbageCollector gc(be, gcAgentRef, getCatalogue());
  gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToTransferForUserAutoGC("ToTransferForUserAutoGC", m_dl);
    cta::objectstore::Agent agentToTransferForUserAutoGC(agentRefToTransferForUserAutoGC.getAgentAddress(), be);
    agentToTransferForUserAutoGC.initialize();
    agentToTransferForUserAutoGC.setTimeout_us(0);
    agentToTransferForUserAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToTransferForUserAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToTransferForUser);
    ar.commit();
    agentRefToTransferForUserAutoGC.addToOwnership(ar.getAddressIfSet(),be);

    ar.garbageCollect(agentRefToTransferForUserAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());
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
    cta::objectstore::AgentReference agentRefToReportToUserForFailure("ToReportToUserForFailure", m_dl);
    cta::objectstore::Agent agentToReportToUserForFailure(agentRefToReportToUserForFailure.getAgentAddress(), be);
    agentToReportToUserForFailure.initialize();
    agentToReportToUserForFailure.setTimeout_us(0);
    agentToReportToUserForFailure.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToTransferForUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForFailure.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForFailure);
    ar.commit();
    sel.release();

    agentRefToReportToUserForFailure.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToUserForFailureAutoGC("ToReportToUserForFailureAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToUserForFailureAutoGC(agentRefToReportToUserForFailureAutoGC.getAgentAddress(), be);
    agentToReportToUserForFailureAutoGC.initialize();
    agentToReportToUserForFailureAutoGC.setTimeout_us(0);
    agentToReportToUserForFailureAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForFailureAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForFailure);
    ar.commit();
    agentRefToReportToUserForFailureAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToUserForFailureAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());

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
    cta::objectstore::AgentReference agentRefToReportToUserForTransfer("ToReportToUserForTransfer", m_dl);
    cta::objectstore::Agent agentToReportToUserForTransfer(agentRefToReportToUserForTransfer.getAgentAddress(), be);
    agentToReportToUserForTransfer.initialize();
    agentToReportToUserForTransfer.setTimeout_us(0);
    agentToReportToUserForTransfer.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForTransfer.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
    ar.commit();
    sel.release();

    agentRefToReportToUserForTransfer.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToUserForTransferAutoGC("ToReportToUserForTransferAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToUserForTransferAutoGC(agentRefToReportToUserForTransferAutoGC.getAgentAddress(), be);
    agentToReportToUserForTransferAutoGC.initialize();
    agentToReportToUserForTransferAutoGC.setTimeout_us(0);
    agentToReportToUserForTransferAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToUserForTransferAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
    ar.commit();
    agentRefToReportToUserForTransferAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToUserForTransferAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());

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
    cta::objectstore::AgentReference agentRefFailed("Failed", m_dl);
    cta::objectstore::Agent agentFailed(agentRefFailed.getAgentAddress(), be);
    agentFailed.initialize();
    agentFailed.setTimeout_us(0);
    agentFailed.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::JobsToReportToUser), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefFailed.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_Failed);
    ar.commit();
    sel.release();

    agentRefFailed.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefFailedAutoGC("FailedAutoGC", m_dl);
    cta::objectstore::Agent agentFailedAutoGC(agentRefFailedAutoGC.getAgentAddress(), be);
    agentFailedAutoGC.initialize();
    agentFailedAutoGC.setTimeout_us(0);
    agentFailedAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefFailedAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_Failed);
    ar.commit();
    agentRefFailedAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefFailedAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());

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
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccess("ToReportToUserForTransfer", m_dl);
    cta::objectstore::Agent agentToReportToRepackForSuccess(agentRefToReportToRepackForSuccess.getAgentAddress(), be);
    agentToReportToRepackForSuccess.initialize();
    agentToReportToRepackForSuccess.setTimeout_us(0);
    agentToReportToRepackForSuccess.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(tapePool, JobQueueType::FailedJobs), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForSuccess.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForSuccess);
    ar.commit();
    sel.release();

    agentRefToReportToRepackForSuccess.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToRepackForSuccessAutoGC("ToReportToRepackForSuccessAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToRepackForSuccessAutoGC(agentRefToReportToRepackForSuccessAutoGC.getAgentAddress(), be);
    agentToReportToRepackForSuccessAutoGC.initialize();
    agentToReportToRepackForSuccessAutoGC.setTimeout_us(0);
    agentToReportToRepackForSuccessAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForSuccessAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForSuccess);
    ar.commit();
    agentRefToReportToRepackForSuccessAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToRepackForSuccessAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());

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
    cta::objectstore::AgentReference agentRefToReportToRepackForFailure("ToReportToRepackForFailure", m_dl);
    cta::objectstore::Agent agentToReportToRepackForFailure(agentRefToReportToRepackForFailure.getAgentAddress(), be);
    agentToReportToRepackForFailure.initialize();
    agentToReportToRepackForFailure.setTimeout_us(0);
    agentToReportToRepackForFailure.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForSuccess), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();


    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForFailure.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForFailure);
    ar.commit();
    sel.release();

    agentRefToReportToRepackForFailure.addToOwnership(ar.getAddressIfSet(),be);

    gc.runOnePass(m_lc);

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
    cta::objectstore::AgentReference agentRefToReportToRepackForFailureAutoGC("ToReportToRepackForFailureAutoGC", m_dl);
    cta::objectstore::Agent agentToReportToRepackForFailureAutoGC(agentRefToReportToRepackForFailureAutoGC.getAgentAddress(), be);
    agentToReportToRepackForFailureAutoGC.initialize();
    agentToReportToRepackForFailureAutoGC.setTimeout_us(0);
    agentToReportToRepackForFailureAutoGC.insertAndRegisterSelf(m_lc);

    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(ri.repackRequestAddress, JobQueueType::JobsToReportToRepackForFailure), be);
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    aq.removeJobsAndCommit({ar.getAddressIfSet()}, m_lc);
    aql.release();

    cta::objectstore::ScopedExclusiveLock sel(ar);
    ar.fetch();
    ar.setJobOwner(2,agentRefToReportToRepackForFailureAutoGC.getAgentAddress());
    ar.setJobStatus(2,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToRepackForFailure);
    ar.commit();
    agentRefToReportToRepackForFailureAutoGC.addToOwnership(ar.getAddressIfSet(),be);
    ar.garbageCollect(agentRefToReportToRepackForFailureAutoGC.getAgentAddress(),getAgentRef(),m_lc,getCatalogue());

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
