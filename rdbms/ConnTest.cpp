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

#include "common/exception/Exception.hpp"
#include "rdbms/ConnFactoryFactory.hpp"
#include "rdbms/ConnPool.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_ConnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_ConnTest, createSameTableInTwoSeparateInMemoryDatabases_executeNonQuery) {
  using namespace cta::rdbms;

  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";

  // First in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared");
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    conn->executeNonQuery(sql, Stmt::AutocommitMode::ON);

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared");
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    conn->executeNonQuery(sql, Stmt::AutocommitMode::ON);

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

TEST_F(cta_rdbms_ConnTest, createSameTableInTwoSeparateInMemoryDatabases_executeNonQueries) {
  using namespace cta::rdbms;

  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER);";

  // First in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared");
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    conn->executeNonQueries(sql);

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared");
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    conn->executeNonQueries(sql);

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

} // namespace unitTests
