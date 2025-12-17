/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_wrapper_ConnTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_rdbms_wrapper_ConnTest, createSameTableInTwoSeparateInMemoryDatabases) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  const char* const sql = R"SQL(
    CREATE TABLE POOLED_STMT_TEST(ID INTEGER)
  )SQL";

  // First in-memory database
  {
    const rdbms::Login login = rdbms::Login::getInMemory();
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const rdbms::Login login = rdbms::Login::getInMemory();
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

}  // namespace unitTests
