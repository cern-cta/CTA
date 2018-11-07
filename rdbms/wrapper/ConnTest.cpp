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
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_wrapper_ConnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_wrapper_ConnTest, createSameTableInTwoSeparateInMemoryDatabases) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";

  // First in-memory database
  {
    const rdbms::Login login(rdbms::Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql, rdbms::AutocommitMode::AUTOCOMMIT_ON);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const rdbms::Login login(rdbms::Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql, rdbms::AutocommitMode::AUTOCOMMIT_ON);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

} // namespace unitTests
