#include "catalogue/SqliteConn.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_SqliteConnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_SqliteConnTest, constructor) {
  using namespace cta::catalogue;

  std::unique_ptr<SqliteConn> db;
  ASSERT_NO_THROW(db.reset(new SqliteConn(":memory:")));
}

TEST_F(cta_catalogue_SqliteConnTest, enableForeignKeys) {
  using namespace cta::catalogue;

  std::unique_ptr<SqliteConn> db;
  ASSERT_NO_THROW(db.reset(new SqliteConn(":memory:")));
  ASSERT_NO_THROW(db->enableForeignKeys());
}

TEST_F(cta_catalogue_SqliteConnTest, createTable) {
  using namespace cta::catalogue;

  std::unique_ptr<SqliteConn> db;
  ASSERT_NO_THROW(db.reset(new SqliteConn(":memory:")));
  ASSERT_NO_THROW(db->execNonQuery("CREATE TABLE TEST(COL1 INTEGER);"));
}

TEST_F(cta_catalogue_SqliteConnTest, createSameTableTwice) {
  using namespace cta::catalogue;

  std::unique_ptr<SqliteConn> db;
  ASSERT_NO_THROW(db.reset(new SqliteConn(":memory:")));
  ASSERT_NO_THROW(db->execNonQuery("CREATE TABLE TEST(COL1 INTEGER);"));
  ASSERT_THROW(db->execNonQuery("CREATE TABLE TEST(COL1 INTEGER);"),
    cta::exception::Exception);
}

} // namespace unitTests
