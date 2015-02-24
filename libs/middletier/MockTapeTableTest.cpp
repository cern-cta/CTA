#include "MockTapeTable.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockTapeTableTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockTapeTableTest, createTape_new) {
  using namespace cta;

  MockTapeTable table;
  const SecurityIdentity requester;

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string poolName = "TestTapePool";
  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(table.createTape(requester, vid, libraryName, poolName,
    capacityInBytes, tapeComment));

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_EQ(1, tapes.size());

    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(poolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  }
}

TEST_F(cta_client_MockTapeTableTest, createTape_already_existing) {
  using namespace cta;

  MockTapeTable table;
  const SecurityIdentity requester;

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string poolName = "TestTapePool";
  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(table.createTape(requester, vid, libraryName, poolName,
    capacityInBytes, tapeComment));

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_EQ(1, tapes.size());

    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(poolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  }
  
  ASSERT_THROW(table.createTape(requester, vid, libraryName, poolName,
    capacityInBytes, tapeComment), std::exception);

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_EQ(1, tapes.size());

    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(poolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  }
}

TEST_F(cta_client_MockTapeTableTest, createTape_lexicographical_order) {
  using namespace cta;

  MockTapeTable table;
  const SecurityIdentity requester;

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string poolName = "TestTapePool";
  const uint64_t capacityInBytes = 12345678;
  ASSERT_NO_THROW(table.createTape(requester, "d", libraryName, poolName, capacityInBytes,
    "Comment d"));
  ASSERT_NO_THROW(table.createTape(requester, "b", libraryName, poolName, capacityInBytes,
    "Comment b"));
  ASSERT_NO_THROW(table.createTape(requester, "a", libraryName, poolName, capacityInBytes,
    "Comment a"));
  ASSERT_NO_THROW(table.createTape(requester, "c", libraryName, poolName, capacityInBytes,
    "Comment c"));

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_EQ(4, tapes.size());

    ASSERT_EQ(std::string("a"), tapes.front().getVid());
    tapes.pop_front();
    ASSERT_EQ(std::string("b"), tapes.front().getVid());
    tapes.pop_front();
    ASSERT_EQ(std::string("c"), tapes.front().getVid());
    tapes.pop_front();
    ASSERT_EQ(std::string("d"), tapes.front().getVid());
  }
}

TEST_F(cta_client_MockTapeTableTest, deleteTape_existing) {
  using namespace cta;

  MockTapeTable table;
  const SecurityIdentity requester;

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string poolName = "TestTapePool";
  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(table.createTape(requester, vid, libraryName, poolName,
    capacityInBytes, tapeComment));

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_EQ(1, tapes.size());

    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(poolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  }
  
  ASSERT_THROW(table.createTape(requester, vid, libraryName, poolName,
    capacityInBytes, tapeComment), std::exception);

  ASSERT_NO_THROW(table.deleteTape(requester, vid));

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }
}

TEST_F(cta_client_MockTapeTableTest, deleteTape_non_existing) {
  using namespace cta;

  MockTapeTable table;
  const SecurityIdentity requester;

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string vid = "TestVid";
  ASSERT_THROW(table.deleteTape(requester, vid), std::exception);

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = table.getTapes(requester));
    ASSERT_TRUE(tapes.empty());
  }
}

} // namespace unitTests
