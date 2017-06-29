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

#include "SqliteConn.hpp"
#include "SqliteRsetImpl.hpp"
#include "SqliteStmt.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_rdbms_SqliteStmtTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_SqliteStmtTest, create_table) {
  using namespace cta::rdbms;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  // Assert that there are currently noi tables in the database
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) NB_TABLES "
      "FROM "
        "SQLITE_MASTER "
      "WHERE "
        "TYPE = 'table';";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());
    const uint64_t nbTables = rset.columnUint64("NB_TABLES");
    ASSERT_EQ(0, nbTables);
    ASSERT_FALSE(rset.next());
    ASSERT_TRUE(conn.getTableNames().empty());
  }

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST1("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
  }

  // Test for the existence of the test table
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) NB_TABLES "
      "FROM SQLITE_MASTER "
      "WHERE "
        "NAME = 'TEST1' AND "
        "TYPE = 'table';";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());
    const uint64_t nbTables = rset.columnUint64("NB_TABLES");
    ASSERT_EQ(1, nbTables);
    ASSERT_FALSE(rset.next());
    ASSERT_EQ(1, conn.getTableNames().size());
    ASSERT_EQ("TEST1", conn.getTableNames().front());
  }

  // Create a second test table
  {
    const char *const sql =
      "CREATE TABLE TEST2("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
  }

  // Test for the existence of the second test table
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) NB_TABLES "
      "FROM SQLITE_MASTER "
      "WHERE "
        "NAME = 'TEST2' AND "
        "TYPE = 'table';";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());
    const uint64_t nbTables = rset.columnUint64("NB_TABLES");
    ASSERT_EQ(1, nbTables);
    ASSERT_FALSE(rset.next());
    const auto tableNames = conn.getTableNames();
    ASSERT_EQ(2, tableNames.size());
    auto nameItor = tableNames.begin();
    ASSERT_EQ("TEST1", *nameItor);
    nameItor++;
    ASSERT_EQ("TEST2", *nameItor);
    nameItor++;
    ASSERT_EQ(tableNames.end(), nameItor);
  }
}

TEST_F(cta_rdbms_SqliteStmtTest, select_from_empty_table) {
  using namespace cta::rdbms;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  ASSERT_TRUE(conn.getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
    ASSERT_EQ(1, conn.getTableNames().size());
    ASSERT_EQ("TEST", conn.getTableNames().front());
  }

  // Select from the empty table
  {
    const char *const sql =
      "SELECT "
        "COL1,"
        "COL2,"
        "COL3 "
      "FROM "
        "TEST;";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_FALSE(rset.next());
  }
}

TEST_F(cta_rdbms_SqliteStmtTest, insert_without_bind) {
  using namespace cta::rdbms;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  ASSERT_TRUE(conn.getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
    ASSERT_EQ(1, conn.getTableNames().size());
    ASSERT_EQ("TEST", conn.getTableNames().front());
  }

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1,"
        "COL2,"
        "COL3)"
      "VALUES("
        "'one',"
        "'two',"
        "3);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1,"
        "COL2 AS COL2,"
        "COL3 AS COL3 "
      "FROM "
        "TEST;";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());

    const std::string col1 = rset.columnString("COL1");
    const std::string col2 = rset.columnString("COL2");
    const uint64_t col3 = rset.columnUint64("COL3");

    ASSERT_EQ("one", col1);
    ASSERT_EQ("two", col2);
    ASSERT_EQ((uint64_t)3, col3);

    ASSERT_FALSE(rset.next());
  }
}

TEST_F(cta_rdbms_SqliteStmtTest, insert_with_bind) {
  using namespace cta::rdbms;

  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  ASSERT_TRUE(conn.getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
    ASSERT_EQ(1, conn.getTableNames().size());
    ASSERT_EQ("TEST", conn.getTableNames().front());
  }

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1,"
        "COL2,"
        "COL3)"
      "VALUES("
        ":COL1,"
        ":COL2,"
        ":COL3);";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->bindString(":COL1", "one");
    stmt->bindString(":COL2", "two");
    stmt->bindUint64(":COL3", 3);
    stmt->executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1,"
        "COL2 AS COL2,"
        "COL3 AS COL3 "
      "FROM "
        "TEST;";
    auto stmt = conn.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());

    const std::string col1 = rset.columnString("COL1");
    const std::string col2 = rset.columnString("COL2");
    const uint64_t col3 = rset.columnUint64("COL3");

    ASSERT_EQ("one", col1);
    ASSERT_EQ("two", col2);
    ASSERT_EQ((uint64_t)3, col3);

    ASSERT_FALSE(rset.next());
  }
}

TEST_F(cta_rdbms_SqliteStmtTest, isolated_transaction) {
  using namespace cta::rdbms;

  const std::string dbFilename = "file::memory:?cache=shared";

  // Create a table in an in-memory resident database
  SqliteConn connForCreate(dbFilename);
  ASSERT_TRUE(connForCreate.getTableNames().empty());
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = connForCreate.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
    ASSERT_EQ(1, connForCreate.getTableNames().size());
    ASSERT_EQ("TEST", connForCreate.getTableNames().front());
  }

  // Insert a row but do not commit using a separate connection
  SqliteConn connForInsert(dbFilename);
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1,"
        "COL2,"
        "COL3)"
      "VALUES("
        "'one',"
        "'two',"
        "3);";
    auto stmt = connForInsert.createStmt(sql, Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
  }

  // Count the number of rows in the table from within another connection
  SqliteConn connForSelect(dbFilename);
  ASSERT_EQ(1, connForSelect.getTableNames().size());
  ASSERT_EQ("TEST", connForSelect.getTableNames().front());
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROWS "
      "FROM "
        "TEST;";
    auto stmt = connForSelect.createStmt(sql, Stmt::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset.next());

    const uint64_t nbRows = rset.columnUint64("NB_ROWS");

    ASSERT_EQ((uint64_t)1, nbRows);

    ASSERT_FALSE(rset.next());
  }
}

} // namespace unitTests
