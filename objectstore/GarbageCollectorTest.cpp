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

TEST(GarbageCollector, BasicFuctionnality) {
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
  cta::objectstore::CreationLog cl(99, 99, "unittesthost", time(NULL),
    "Creation of unit test agent register");
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

TEST(GarbageCollector, AgentRegister) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::CreationLog cl(99, 99, "unittesthost", time(NULL),
    "Creation of unit test agent register");
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

TEST(GarbageCollector, TapePool) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::CreationLog cl(99, 99, "unittesthost", time(NULL),
    "Creation of unit test agent register");
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

TEST(GarbageCollector, DriveRegister) {
  // Here we check that can successfully call agentRegister's garbage collector
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::CreationLog cl(99, 99, "unittesthost", time(NULL),
    "Creation of unit test agent register");
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


}
