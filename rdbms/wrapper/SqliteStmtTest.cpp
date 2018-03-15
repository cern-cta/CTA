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

#include "common/exception/DatabaseConstraintViolation.hpp"
#include "rdbms/wrapper/SqliteConn.hpp"
#include "rdbms/wrapper/SqliteRset.hpp"
#include "rdbms/wrapper/SqliteStmt.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_rdbms_wrapper_SqliteStmtTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, create_table) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());
    const auto nbTables = rset->columnOptionalUint64("NB_TABLES");
    ASSERT_TRUE((bool)nbTables);
    ASSERT_EQ(0, nbTables.value());
    ASSERT_FALSE(rset->next());
    ASSERT_TRUE(conn.getTableNames().empty());
  }

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST1("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());
    const auto nbTables = rset->columnOptionalUint64("NB_TABLES");
    ASSERT_TRUE((bool)nbTables);
    ASSERT_EQ(1, nbTables.value());
    ASSERT_FALSE(rset->next());
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());
    const auto nbTables = rset->columnOptionalUint64("NB_TABLES");
    ASSERT_TRUE((bool)nbTables);
    ASSERT_EQ(1, nbTables.value());
    ASSERT_FALSE(rset->next());
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

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, select_from_empty_table) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, insert_without_bind) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto col1 = rset->columnOptionalString("COL1");
    const auto col2 = rset->columnOptionalString("COL2");
    const auto col3 = rset->columnOptionalUint64("COL3");

    ASSERT_TRUE((bool)col1);
    ASSERT_TRUE((bool)col2);
    ASSERT_TRUE((bool)col3);

    ASSERT_EQ("one", col1.value());
    ASSERT_EQ("two", col2.value());
    ASSERT_EQ(3, col3.value());

    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, insert_with_bind) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto col1 = rset->columnOptionalString("COL1");
    const auto col2 = rset->columnOptionalString("COL2");
    const auto col3 = rset->columnOptionalUint64("COL3");

    ASSERT_TRUE((bool)col1);
    ASSERT_TRUE((bool)col2);
    ASSERT_TRUE((bool)col3);

    ASSERT_EQ("one", col1.value());
    ASSERT_EQ("two", col2.value());
    ASSERT_EQ(3, col3.value());

    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, isolated_transaction) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

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
    auto stmt = connForCreate.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = connForInsert.createStmt(sql, rdbms::AutocommitMode::ON);
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
    auto stmt = connForSelect.createStmt(sql, rdbms::AutocommitMode::ON);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto nbRows = rset->columnOptionalUint64("NB_ROWS");
    ASSERT_TRUE((bool)nbRows);
    ASSERT_EQ((uint64_t)1, nbRows.value());

    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_rdbms_wrapper_SqliteStmtTest, insert_violating_primary_key) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  ASSERT_TRUE(conn.getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 INTEGER,"
        "CONSTRAINT TEST_COL1_PK PRIMARY KEY(COL1));";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt->executeNonQuery();
    ASSERT_EQ(1, conn.getTableNames().size());
    ASSERT_EQ("TEST", conn.getTableNames().front());
  }

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1)"
      "VALUES("
        ":COL1);";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt->bindUint64(":COL1", 1);
    stmt->executeNonQuery();
  }

  // Try to insert an identical row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1)"
      "VALUES("
        ":COL1);";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::ON);
    stmt->bindUint64(":COL1", 1);
    ASSERT_THROW(stmt->executeNonQuery(), exception::DatabaseConstraintViolation);
  }
}

} // namespace unitTests
