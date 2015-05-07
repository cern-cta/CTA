/**
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
#include "exception/Exception.hpp"
#include "GarbageCollector.hpp"
#include "FIFO.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"

namespace unitTests {

TEST(GarbageCollector, BasicFuctionnality) {
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTestGarbageCollector");
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  re.allocateOrGetAgentRegister(agent);
  // Create 2 agents, A and B and register them
  cta::objectstore::Agent agA(be), agB(be);
  agA.initialize();
  agA.generateName("unitTestAgentA");
  agA.insertAndRegisterSelf();
  agB.initialize();
  agB.generateName("unitTestAgentB");
  agB.insertAndRegisterSelf();
  // Create target FIFO
  std::string fifoName = agent.nextId("FIFO");
  std::list<std::string> expectedData;
  // Try to create the FIFO entry
  cta::objectstore::FIFO ff(fifoName,be);
  ff.initialize();
  ff.insert();
  // And lock it for later
  cta::objectstore::ScopedExclusiveLock ffLock;
  {
    for (int i=0; i<100; i++) {
      // We create FIFOs here, but any object can do.
      // Create a new object
      cta::objectstore::FIFO newFIFO(agent.nextId("RandomObject"), be);
      // Small shortcut: insert the link to the new object straight into the FIFO
      cta::objectstore::FIFO centralFifo(fifoName, be);
      cta::objectstore::ScopedExclusiveLock lock(centralFifo);
      centralFifo.fetch();
      expectedData.push_back(newFIFO.getNameIfSet());
      centralFifo.push(expectedData.back());
      centralFifo.commit();
      lock.release();
      // Then actually create the object
      newFIFO.initialize();
      newFIFO.setOwner(fifoName);
      newFIFO.setBackupOwner(fifoName);
      newFIFO.insert();
    }
  }
  ffLock.lock(ff);
  ff.fetch();
  ASSERT_EQ(100, ff.size());
  ffLock.release();
  for (int i=0; i<10; i++) {
    cta::objectstore::ScopedExclusiveLock objALock, objBLock;
    cta::objectstore::FIFO objA(be), objB(be); 
    agA.popFromContainer(ff, objA, objALock);
    agB.popFromContainer(ff, objB, objBLock);
  }
  ffLock.lock(ff);
  ff.fetch();
  ASSERT_EQ(80, ff.size());
  ffLock.release();
  // Create the garbage colletor and run it twice.
  cta::objectstore::Agent gcAgent(be);
  gcAgent.initialize();
  gcAgent.generateName("unitTestGarbageCollector");
  gcAgent.insertAndRegisterSelf();
  cta::objectstore::GarbageCollector gc(be, gcAgent);
  gc.setTimeout(0);
  gc.runOnePass();
  gc.runOnePass();
  ffLock.lock(ff);
  ff.fetch();
  ASSERT_EQ(100, ff.size());
}

}
