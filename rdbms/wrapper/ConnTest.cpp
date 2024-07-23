/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

  const char* const sql = R"SQL(
    CREATE TABLE POOLED_STMT_TEST(ID INTEGER)
  )SQL";

  // First in-memory database
  {
    const rdbms::Login login(rdbms::Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const rdbms::Login login(rdbms::Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    ASSERT_TRUE(conn->getTableNames().empty());

    auto stmt = conn->createStmt(sql);
    stmt->executeNonQuery();

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

} // namespace unitTests
