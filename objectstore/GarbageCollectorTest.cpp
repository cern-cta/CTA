#include <gtest/gtest.h>
#include "BackendVFS.hpp"
#include "exception/Exception.hpp"
#include "GarbageCollector.hpp"
#include "FIFO.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "RootEntry.hpp"

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
  agA.insert();
  agA.insertAndRegisterSelf();
  agB.initialize();
  agB.generateName("unitTestAgentB");
  agB.insert();
  agB.insertAndRegisterSelf();
  // Create target FIFO
  std::string fifoName = agent.nextId("FIFO");
  std::list<std::string> expectedData;
  { 
    // Try to create the FIFO entry
    cta::objectstore::FIFO ff(fifoName,be);
    ff.initialize();
    ff.insert();
  }
  {
    for (int i=0; i<100; i++) {
      // We create FIFOs here, but any object can do.
      // Create a new object
      cta::objectstore::FIFO newFIFO(agent.nextId("RandomObject"), be);
      // Small shortcut: insert the link to the new object straight into the FIFO
      cta::objectstore::FIFO centralFifo(fifoName, be);
      cta::objectstore::ScopedExclusiveLock lock(centralFifo);
      centralFifo.fetch();
      expectedData.push_back(agent.nextId("TestData"));
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
  for (int i=0; i<10; i++) {
    
  }
  // TODO: take ownership of FIFO contents in agA and agB, and then garbage collect.
  // The FIFO should get all its objects back.
}