#include "MockTapePoolTable.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockTapePoolTableTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockTapePoolTableTest, createTapePool_new) {
  using namespace cta;

  MockTapePoolTable db;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";

  ASSERT_THROW(db.checkTapePoolExists(tapePoolName), std::exception);

  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createTapePool(requester, tapePoolName, nbDrives, 
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  }

  ASSERT_NO_THROW(db.checkTapePoolExists(tapePoolName));
}

TEST_F(cta_client_MockTapePoolTableTest, createTapePool_already_existing) {
  using namespace cta;

  MockTapePoolTable db;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createTapePool(requester, tapePoolName, nbDrives, 
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  }
  
  ASSERT_THROW(db.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment), std::exception);
}

TEST_F(cta_client_MockTapePoolTableTest, createTapePool_lexicographical_order) {
  using namespace cta;

  MockTapePoolTable db;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;

  ASSERT_NO_THROW(db.createTapePool(requester, "d", nbDrives, nbPartialTapes,
    "Comment d"));
  ASSERT_NO_THROW(db.createTapePool(requester, "b", nbDrives, nbPartialTapes,
    "Comment b"));
  ASSERT_NO_THROW(db.createTapePool(requester, "a", nbDrives, nbPartialTapes,
    "Comment a"));
  ASSERT_NO_THROW(db.createTapePool(requester, "c", nbDrives, nbPartialTapes,
    "Comment c"));
  
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_EQ(4, tapePools.size());

    ASSERT_EQ(std::string("a"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("b"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("c"), tapePools.front().getName());
    tapePools.pop_front();
    ASSERT_EQ(std::string("d"), tapePools.front().getName());
  }
}

TEST_F(cta_client_MockTapePoolTableTest, deleteTapePool_existing) {
  using namespace cta;

  MockTapePoolTable db;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbDrives = 1;
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createTapePool(requester, tapePoolName, nbDrives,
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_EQ(1, tapePools.size());
  
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());

    ASSERT_NO_THROW(db.deleteTapePool(requester, tapePoolName));
  }

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

TEST_F(cta_client_MockTapePoolTableTest, deleteTapePool_non_existing) {
  using namespace cta;

  MockTapePoolTable db;
  const SecurityIdentity requester;

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }

  const std::string name = "TestTapePool";
  ASSERT_THROW(db.deleteTapePool(requester, name), std::exception);

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = db.getTapePools(requester));
    ASSERT_TRUE(tapePools.empty());
  }
}

} // namespace unitTests
