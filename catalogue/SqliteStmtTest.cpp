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

#include "catalogue/SqliteConn.hpp"
#include "catalogue/SqliteRset.hpp"
#include "catalogue/SqliteStmt.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_SqliteStmtTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_SqliteStmtTest, create_table) {
  using namespace cta::catalogue;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  // Assert that there are currently noi tables in the database
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) NB_TABLES "
        "FROM SQLITE_MASTER "
        "WHERE "
        "TYPE = 'table';";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    ASSERT_TRUE(rset->next());
    const uint64_t nbTables = rset->columnUint64("NB_TABLES");
    ASSERT_EQ(0, nbTables);
    ASSERT_FALSE(rset->next());
  }

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    stmt->executeNonQuery();
  }

  // Test for the existence of the test table
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) NB_TABLES "
      "FROM SQLITE_MASTER "
      "WHERE "
        "NAME = 'TEST' AND "
        "TYPE = 'table';";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    ASSERT_TRUE(rset->next());
    const uint64_t nbTables = rset->columnUint64("NB_TABLES");
    ASSERT_EQ(1, nbTables);
    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_catalogue_SqliteStmtTest, select_from_empty_table) {
  using namespace cta::catalogue;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    stmt->executeNonQuery();
  }

  // Select from the empty table
  {
    const char *const sql =
      "SELECT "
        "COL1,"
        "COL2,"
        "COL3 "
      "FROM TEST;";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_catalogue_SqliteStmtTest, insert_without_bind) {
  using namespace cta::catalogue;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
     std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
     stmt->executeNonQuery();
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
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    stmt->executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1,"
        "COL2 AS COL2,"
        "COL3 AS COL3 "
      "FROM TEST;";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    ASSERT_TRUE(rset->next());

    std::string col1;
    std::string col2;
    uint64_t col3 = 0;

    ASSERT_NO_THROW(col1 = rset->columnText("COL1"));
    ASSERT_NO_THROW(col2 = rset->columnText("COL2"));
    ASSERT_NO_THROW(col3 = rset->columnUint64("COL3"));

    ASSERT_EQ("one", col1);
    ASSERT_EQ("two", col2);
    ASSERT_EQ((uint64_t)3, col3);

    ASSERT_FALSE(rset->next());
  }
}

TEST_F(cta_catalogue_SqliteStmtTest, insert_with_bind) {
  using namespace cta::catalogue;
  // Create a connection a memory resident database
  SqliteConn conn(":memory:");

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 TEXT,"
        "COL2 TEXT,"
        "COL3 INTEGER);";
     std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
     stmt->executeNonQuery();
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
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    ASSERT_NO_THROW(stmt->bindString(":COL1", "one"));
    ASSERT_NO_THROW(stmt->bindString(":COL2", "two"));
    ASSERT_NO_THROW(stmt->bindUint64(":COL3", 3));
    stmt->executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1,"
        "COL2 AS COL2,"
        "COL3 AS COL3 "
      "FROM TEST;";
    std::unique_ptr<DbStmt> stmt(conn.createStmt(sql));
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    ASSERT_TRUE(rset->next());

    std::string col1;
    std::string col2;
    uint64_t col3 = 0;

    ASSERT_NO_THROW(col1 = rset->columnText("COL1"));
    ASSERT_NO_THROW(col2 = rset->columnText("COL2"));
    ASSERT_NO_THROW(col3 = rset->columnUint64("COL3"));

    ASSERT_EQ("one", col1);
    ASSERT_EQ("two", col2);
    ASSERT_EQ((uint64_t)3, col3);

    ASSERT_FALSE(rset->next());
  }
}

} // namespace unitTests
