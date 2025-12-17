/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/Rset.hpp"

#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_RsetTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_rdbms_RsetTest, constructor) {
  using namespace cta::rdbms;

  Rset rset;

  ASSERT_TRUE(rset.isEmpty());
}

TEST_F(cta_rdbms_RsetTest, next) {
  using namespace cta::rdbms;

  const Login login = Login::getInMemory();
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  StmtPool pool;
  {
    const char* const sql = R"SQL(
      CREATE TABLE RSET_TEST(ID INTEGER)
    )SQL";
    Stmt stmt = pool.getStmt(*conn, sql);
    stmt.executeNonQuery();
  }

  {
    const char* const sql = R"SQL(
      INSERT INTO RSET_TEST(ID) VALUES(1)
    )SQL";
    Stmt stmt = pool.getStmt(*conn, sql);
    stmt.executeNonQuery();
  }

  {
    const char* const sql = R"SQL(
      SELECT ID AS ID FROM RSET_TEST ORDER BY ID
    )SQL";
    Stmt stmt = pool.getStmt(*conn, sql);
    auto rset = stmt.executeQuery();

    ASSERT_FALSE(rset.isEmpty());
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1, rset.columnUint64("ID"));

    ASSERT_FALSE(rset.next());

    ASSERT_THROW(rset.next(), InvalidResultSet);

    ASSERT_TRUE(rset.isEmpty());
  }
}

}  // namespace unitTests
