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
#include "GarbageCollector.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "TapePool.hpp"
#include "DriveRegister.hpp"

namespace unitTests {

TEST(ObjectStore, GarbageCollectorBasicFuctionnality) {
  // Here we check for the ability to detect dead (but empty agents)
  // and clean them up.
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
  rel.release();
  // Create 2 agents, A and B and register them
  // The agents are set with a timeout of 0, so they will be delclared
  // dead immediately.
  cta::objectstore::Agent agA(be), agB(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  agB.initialize();
  agB.generateName("unitTestAgentB");
  agB.setTimeout_us(0);
  agB.insertAndRegisterSelf();
  // Create the garbage colletor and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
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
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::Agent agA(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new agent register, owned by agA (by hand as it is not an usual
  // situation)
  std::string arName;
  {
    arName = agA.nextId("AgentRegister");
    cta::objectstore::AgentRegister ar(arName, be);
    ar.initialize();
    ar.setOwner(agA.getAddressIfSet());
    cta::objectstore::ScopedExclusiveLock agl(agA);
    agA.fetch();
    agA.addToOwnership(arName);
    agA.commit();
    ar.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
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

TEST(ObjectStore, GarbageCollectorTapePool) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
  rel.release();
  // Create an agent and add and agent register to it as an owned object
  cta::objectstore::Agent agA(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new agent register, owned by agA (by hand as it is not an usual
  // situation)
  std::string tpName;
  {
    tpName = agA.nextId("TapePool");
    cta::objectstore::TapePool tp(tpName, be);
    tp.initialize("SomeTP");
    tp.setOwner(agA.getAddressIfSet());
    cta::objectstore::ScopedExclusiveLock agl(agA);
    agA.fetch();
    agA.addToOwnership(tpName);
    agA.commit();
    tp.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
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
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
  rel.release();
  // Create an agent and add the drive register to it as an owned object
  cta::objectstore::Agent agA(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Create a new drive register, owned by agA (by hand as it is not an usual
  // situation)
  std::string tpName;
  {
    tpName = agA.nextId("TapePool");
    cta::objectstore::DriveRegister dr(tpName, be);
    dr.initialize();
    dr.setOwner(agA.getAddressIfSet());
    cta::objectstore::ScopedExclusiveLock agl(agA);
    agA.fetch();
    agA.addToOwnership(tpName);
    agA.commit();
    dr.insert();
  }
  // Create the garbage colletor and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
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

TEST(ObjectStore, GarbageCollectorArchiveToFileRequest) {
  // Here we check that can successfully call ArchiveToFileRequests's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::CreationLog cl(cta::UserIdentity(99, 99),
      "unittesthost", time(NULL), "Creation of unit test agent register");
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agent, cl);
  rel.release();
  // Create an agent
  cta::objectstore::Agent agA(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.setTimeout_us(0);
  agA.insertAndRegisterSelf();
  // Several use cases are present for the ArchiveToFileRequests:
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
    std::stringstream tpid;
    tpid << "TapePool" << i;
    tpAddr[i] = agent.nextId(tpid.str());
    cta::objectstore::TapePool tp(tpAddr[i], be);
    tp.initialize(tpid.str());
    tp.setOwner("");
    tp.insert();
  }
  // Create the various ATFR's, stopping one step further each time.
  int pass=0;
  while (true)
  {
    // -just referenced
    std::string atfrAddr = agA.nextId("ArchiveToFileRequest");
    cta::objectstore::ScopedExclusiveLock agl(agA);
    agA.fetch();
    agA.addToOwnership(atfrAddr);
    agA.commit();
    if (pass < 1) { pass++; continue; }
    // - created, but not linked to tape pools
    cta::objectstore::ArchiveToFileRequest atfr(atfrAddr, be);
    atfr.initialize();
    atfr.setArchiveFile("cta:/file");
    atfr.setRemoteFile("eos:/file");
    atfr.setPriority(0);
    atfr.setSize(1000+pass);
    cta::objectstore::CreationLog log(cta::objectstore::UserIdentity(123,456),
        "unitTestHost", time(NULL), "ArchiveJobForGarbageCollection");
    atfr.setCreationLog(log);
    atfr.addJob(1, "TapePool0", tpAddr[0]);
    atfr.addJob(2, "TapePool1", tpAddr[1]);    
    atfr.setOwner(agA.getAddressIfSet());
    atfr.insert();
    cta::objectstore::ScopedExclusiveLock atfrl(atfr);
    if (pass < 2) { pass++; continue; }
    // - Referenced in the first tape pool
    {
      cta::objectstore::TapePool tp(tpAddr[0], be);
      cta::objectstore::ScopedExclusiveLock tpl(tp);
      tp.fetch();
      cta::objectstore::ArchiveToFileRequest::JobDump jd;
      jd.copyNb = 1;
      jd.tapePool = "TapePool0";
      jd.tapePoolAddress = tpAddr[0];
      tp.addJob(jd, atfr.getAddressIfSet(), 1000+pass);
      tp.commit();
    }
    if (pass < 3) { pass++; continue; }
    // TODO: partially migrated or selected
    // - Referenced in the second tape pool
    {
      cta::objectstore::TapePool tp(tpAddr[1], be);
      cta::objectstore::ScopedExclusiveLock tpl(tp);
      tp.fetch();
      cta::objectstore::ArchiveToFileRequest::JobDump jd;
      jd.copyNb = 2;
      jd.tapePool = "TapePool1";
      jd.tapePoolAddress = tpAddr[1];
      tp.addJob(jd, atfr.getAddressIfSet(), 1000);
      tp.commit();
    }
    if (pass < 4) { pass++; continue; }
    // - Still marked a not owned but referenced in the agent
    {
      atfr.setOwner("");
      atfr.commit();
    }
    break;
  }
  // Create the garbage collector and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
  gcAgent.setTimeout_us(0);
  gcAgent.insertAndRegisterSelf();
  {
    cta::objectstore::GarbageCollector gc(be, gcAgent);
    gc.runOnePass();
    gc.runOnePass();
  }
  // All 4 requests should be linked in both tape pools
  {
    cta::objectstore::TapePool tp0(tpAddr[0], be);
    cta::objectstore::ScopedExclusiveLock tp0l(tp0);
    tp0.fetch();
    auto d=tp0.dumpJobs();
    ASSERT_EQ(4, tp0.getJobsSummary().files);
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
