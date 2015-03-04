#include <gtest/gtest.h>
#include "BackendVFS.hpp"
#include "exception/Exception.hpp"
#include "FIFO.hpp"
#include "Agent.hpp"

namespace unitTests {

TEST(FIFO, BasicFuctionnality) {
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTest");
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
      cta::objectstore::FIFO ff(fifoName,be);
      cta::objectstore::ScopedExclusiveLock lock(ff);
      ff.fetch();
      expectedData.push_back(agent.nextId("TestData"));
      ff.push(expectedData.back());
      ff.commit();
    }
  }
  {
    // Check that the data it as expected
    cta::objectstore::FIFO ff(fifoName,be);
    cta::objectstore::ScopedSharedLock lock(ff);
    ff.fetch();
    std::list<std::string> dump = ff.getContent();
    lock.release();
    ASSERT_EQ(expectedData.size(), dump.size());
    std::list<std::string>::const_iterator foundI = dump.begin();
    std::list<std::string>::const_iterator expectI = expectedData.begin();
    while (foundI != dump.end() && expectI != expectedData.end()) {
      ASSERT_EQ(*foundI, *expectI);
      foundI++;
      expectI++;
    }
    ASSERT_TRUE(foundI == dump.end());
    ASSERT_TRUE(expectI == expectedData.end());
    cta::objectstore::ScopedSharedLock lock2(ff);
    ff.fetch();
    //std::cout << 
        ff.dump();
  }
  {
    cta::objectstore::FIFO ff(fifoName,be);
    cta::objectstore::ScopedExclusiveLock lock(ff);
    ff.fetch();
    while (ff.size() && expectedData.size()) {
      ASSERT_EQ(ff.peek(), expectedData.front());
      ff.pop();
      ff.commit();
      ff.fetch();
      expectedData.pop_front();
    }
  }
  // Delete the FIFO entry
  cta::objectstore::FIFO ff(fifoName,be);
  cta::objectstore::ScopedExclusiveLock lock(ff);
  ff.fetch();
  ASSERT_EQ(0, ff.size());
  ff.remove();
  ASSERT_EQ(false, ff.exists());
}

}
