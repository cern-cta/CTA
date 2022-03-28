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
#include "rdbms/ConnPool.hpp"
#include "rdbms/Rset.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_RsetTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_RsetTest, constructor) {
  using namespace cta::rdbms;

  Rset rset;

  ASSERT_TRUE(rset.isEmpty());
}

TEST_F(cta_rdbms_RsetTest, next) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  StmtPool pool;
  {
    const std::string sql = "CREATE TABLE RSET_TEST(ID INTEGER)";
    Stmt stmt = pool.getStmt(*conn, sql);
    stmt.executeNonQuery();
  }
  
  {
    const std::string sql = "INSERT INTO RSET_TEST(ID) VALUES(1)";
    Stmt stmt = pool.getStmt(*conn, sql);
    stmt.executeNonQuery();
  }

  {
    const std::string sql = "SELECT ID AS ID FROM RSET_TEST ORDER BY ID";
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

} // namespace unitTests
