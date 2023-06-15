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
#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_StmtPoolTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_rdbms_StmtPoolTest, getStmt) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";
  StmtPool pool;
  ASSERT_EQ(0, pool.getNbStmts());
  {
    auto stmt = pool.getStmt(*conn, sql);
    ASSERT_EQ(0, pool.getNbStmts());
  }
  ASSERT_EQ(1, pool.getNbStmts());
}

TEST_F(cta_rdbms_StmtPoolTest, moveAssignment) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";
  StmtPool pool;
  ASSERT_EQ(0, pool.getNbStmts());
  {
    Stmt stmt1 = pool.getStmt(*conn, sql);
    Stmt stmt2;
    stmt2 = std::move(stmt1);
    ASSERT_EQ(0, pool.getNbStmts());
  }
  ASSERT_EQ(1, pool.getNbStmts());
}

TEST_F(cta_rdbms_StmtPoolTest, moveConstructor) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";
  StmtPool pool;
  ASSERT_EQ(0, pool.getNbStmts());
  {
    Stmt stmt1 = pool.getStmt(*conn, sql);
    Stmt stmt2(std::move(stmt1));
    ASSERT_EQ(0, pool.getNbStmts());
  }
  ASSERT_EQ(1, pool.getNbStmts());
}

TEST_F(cta_rdbms_StmtPoolTest, createSameTableInTwoSeparateInMemoryDatabases) {
  using namespace cta::rdbms;

  const std::string createTableSql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";
  const std::string selectTableNamesSql = "SELECT "
                                          "NAME AS NAME "
                                          "FROM "
                                          "SQLITE_MASTER "
                                          "WHERE "
                                          "TYPE = 'table' "
                                          "ORDER BY "
                                          "NAME;";

  // First in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = wrapper::ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    StmtPool pool;

    {
      Stmt stmt = pool.getStmt(*conn, selectTableNamesSql);
      auto rset = stmt.executeQuery();
      std::list<std::string> names;
      while (rset.next()) {
        names.push_back(rset.columnString("NAME"));
      }
      ASSERT_EQ(0, names.size());
    }

    {
      Stmt stmt = pool.getStmt(*conn, createTableSql);
      stmt.executeNonQuery();
    }

    {
      Stmt stmt = pool.getStmt(*conn, selectTableNamesSql);
      auto rset = stmt.executeQuery();
      std::list<std::string> names;
      while (rset.next()) {
        names.push_back(rset.columnString("NAME"));
      }
      ASSERT_EQ(1, names.size());
      ASSERT_EQ("POOLED_STMT_TEST", names.front());
    }
  }

  // Second in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = wrapper::ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    StmtPool pool;
    {
      Stmt stmt = pool.getStmt(*conn, selectTableNamesSql);
      auto rset = stmt.executeQuery();
      std::list<std::string> names;
      while (rset.next()) {
        names.push_back(rset.columnString("NAME"));
      }
      ASSERT_EQ(0, names.size());
    }

    {
      Stmt stmt = pool.getStmt(*conn, createTableSql);
      stmt.executeNonQuery();
    }

    {
      Stmt stmt = pool.getStmt(*conn, selectTableNamesSql);
      auto rset = stmt.executeQuery();
      std::list<std::string> names;
      while (rset.next()) {
        names.push_back(rset.columnString("NAME"));
      }
      ASSERT_EQ(1, names.size());
      ASSERT_EQ("POOLED_STMT_TEST", names.front());
    }
  }
}

TEST_F(cta_rdbms_StmtPoolTest, createSameTableInTwoSeparateInMemoryDatabases_getTableNames) {
  using namespace cta::rdbms;

  const std::string createTableSql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";

  // First in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = wrapper::ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    StmtPool pool;

    ASSERT_TRUE(conn->getTableNames().empty());

    {
      Stmt stmt = pool.getStmt(*conn, createTableSql);
      stmt.executeNonQuery();
    }

    ASSERT_EQ(1, conn->getTableNames().size());
  }

  // Second in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    auto connFactory = wrapper::ConnFactoryFactory::create(login);
    auto conn = connFactory->create();

    StmtPool pool;

    ASSERT_TRUE(conn->getTableNames().empty());

    {
      Stmt stmt = pool.getStmt(*conn, createTableSql);
      stmt.executeNonQuery();
    }

    ASSERT_EQ(1, conn->getTableNames().size());
  }
}

TEST_F(cta_rdbms_StmtPoolTest, sameSqlTwoCachedStmts) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  auto connFactory = wrapper::ConnFactoryFactory::create(login);
  auto conn = connFactory->create();
  const std::string sql = "CREATE TABLE POOLED_STMT_TEST(ID INTEGER)";
  StmtPool pool;
  ASSERT_EQ(0, pool.getNbStmts());
  {
    Stmt stmt1 = pool.getStmt(*conn, sql);
    Stmt stmt2 = pool.getStmt(*conn, sql);
    ASSERT_EQ(0, pool.getNbStmts());
  }
  ASSERT_EQ(2, pool.getNbStmts());
}

}  // namespace unitTests
