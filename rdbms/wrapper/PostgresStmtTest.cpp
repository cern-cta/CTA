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

#include "rdbms/ConstraintError.hpp"
#include "rdbms/wrapper/PostgresConn.hpp"
#include "rdbms/wrapper/PostgresRset.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <cstdint>

namespace unitTests {

/**
 *  To enable the test case, just set environment variable GTEST_FILTER
 *  and GTEST_ALSO_RUN_DISABLED_TESTS
 *
 * $ export GTEST_ALSO_RUN_DISABLED_TESTS=1
 * $ export GTEST_FILTER=*Postgres*
 * $ ./tests/cta-unitTests
 */

class DISABLED_cta_rdbms_wrapper_PostgresStmtTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    m_connstring = "postgresql://ctaunittest:ctaunittest@localhost/ctaunittest";
    m_conn = std::make_unique<cta::rdbms::wrapper::PostgresConn>(m_connstring);
    // Try to drop anything owned by ctaunittest currently in the db
    m_conn->executeNonQuery("drop owned by ctaunittest");
    ASSERT_TRUE(m_conn->getTableNames().empty());
  }

  virtual void TearDown() {
    m_conn.reset();
  }

  std::string m_connstring;
  std::unique_ptr<cta::rdbms::wrapper::PostgresConn> m_conn;
};

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, create_table) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST1("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
  }

  // Test for the existence of the test table
  {
    const char *const sql =
       "SELECT COUNT(*) NB_TABLES FROM pg_catalog.pg_tables c "
         "WHERE c.schemaname NOT IN ('pg_catalog', 'information_schema') "
         "AND c.tablename = 'test1'";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());
    const auto nbTables = rset->columnOptionalUint64("NB_TABLES");
    ASSERT_TRUE((bool)nbTables);
    ASSERT_EQ(1, nbTables.value());
    ASSERT_FALSE(rset->next());
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST1", m_conn->getTableNames().front());
  }

  // Create a second test table
  {
    const char *const sql =
      "CREATE TABLE TEST2("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
  }

  // Test for the existence of the second test table
  {
    const char *const sql =
       "SELECT COUNT(*) NB_TABLES FROM pg_catalog.pg_tables c "
         "WHERE c.schemaname NOT IN ('pg_catalog', 'information_schema') "
         "AND c.tablename = 'test2'";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());
    const auto nbTables = rset->columnOptionalUint64("NB_TABLES");
    ASSERT_TRUE((bool)nbTables);
    ASSERT_EQ(1, nbTables.value());
    ASSERT_FALSE(rset->next());
    const auto tableNames = m_conn->getTableNames();
    ASSERT_EQ(2, tableNames.size());
    auto nameItor = tableNames.begin();
    ASSERT_EQ("TEST1", *nameItor);
    nameItor++;
    ASSERT_EQ("TEST2", *nameItor);
    nameItor++;
    ASSERT_EQ(tableNames.end(), nameItor);
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, select_from_empty_table) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0))";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
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
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_FALSE(rset->next());
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, insert_without_bind) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  ASSERT_TRUE(m_conn->getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
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
    auto stmt = m_conn->createStmt(sql);
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
    auto stmt = m_conn->createStmt(sql);
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

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, insert_with_bind) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  ASSERT_TRUE(m_conn->getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
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
    auto stmt = m_conn->createStmt(sql);
    stmt->bindString(":COL1", std::string("one"));
    stmt->bindString(":COL2", std::string("two"));
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
    auto stmt = m_conn->createStmt(sql);
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

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, isolated_transaction) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // The fiest connection, to be used for creating a table
  PostgresConn &connForCreate = *m_conn;

  // Create a table
  ASSERT_TRUE(connForCreate.getTableNames().empty());
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = connForCreate.createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, connForCreate.getTableNames().size());
    ASSERT_EQ("TEST", connForCreate.getTableNames().front());
  }

  // Create second connection to a local database
  // Insert a row but do not commit using the separate connection
  PostgresConn connForInsert(m_connstring);
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
    connForInsert.executeNonQuery("BEGIN");
    auto stmt = connForInsert.createStmt(sql);
    stmt->executeNonQuery();
  }

  // Count the number of rows in the table from within another connection: should be 0
  PostgresConn connForSelect(m_connstring);
  ASSERT_EQ(1, connForSelect.getTableNames().size());
  ASSERT_EQ("TEST", connForSelect.getTableNames().front());
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROWS "
      "FROM "
        "TEST;";
    auto stmt = connForSelect.createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto nbRows = rset->columnOptionalUint64("NB_ROWS");
    ASSERT_TRUE((bool)nbRows);
    ASSERT_EQ((uint64_t)0, nbRows.value());

    ASSERT_FALSE(rset->next());
  }

  //commit on the insert connection
  connForInsert.commit();

  // count the rows again on the select connection: should be 1
  {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROWS "
      "FROM "
        "TEST;";
    auto stmt = connForSelect.createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto nbRows = rset->columnOptionalUint64("NB_ROWS");
    ASSERT_TRUE((bool)nbRows);
    ASSERT_EQ((uint64_t)1, nbRows.value());

    ASSERT_FALSE(rset->next());
  }

}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, executeNonQuery_insert_violating_primary_key) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // Create a connection to a local database
  PostgresConn conn(m_connstring);

  // Try to drop anything owned by ctaunittest currently in the db
  {
    conn.executeNonQuery("drop owned by ctaunittest");
  }

  ASSERT_TRUE(conn.getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 NUMERIC(20,0),"
        "CONSTRAINT TEST_COL1_PK PRIMARY KEY(COL1));";
    auto stmt = conn.createStmt(sql);
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
    auto stmt = conn.createStmt(sql);
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
    auto stmt = conn.createStmt(sql);
    stmt->bindUint64(":COL1", 1);
    ASSERT_THROW(stmt->executeNonQuery(), exception::Exception);
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, executeQuery_insert_violating_primary_key) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  ASSERT_TRUE(m_conn->getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 NUMERIC(20,0),"
        "CONSTRAINT TEST_COL1_PK PRIMARY KEY(COL1));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
  }

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1)"
      "VALUES("
        ":COL1);";
    auto stmt = m_conn->createStmt(sql);
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
    auto stmt = m_conn->createStmt(sql);
    stmt->bindUint64(":COL1", 1);
    auto rset = stmt->executeQuery();
    ASSERT_THROW(rset->next(), exception::Exception);
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, insert_with_large_uint64) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  ASSERT_TRUE(m_conn->getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
  }

  uint64_t val = 0xFFFFFFFFFFFFFFF0ULL;
  // Insert uint64_t with top bit set into a row into the test table
  {
    const char *const sql =
      "INSERT INTO TEST("
        "COL1)"
      "VALUES("
        ":COL1);";
    auto stmt = m_conn->createStmt(sql);
    stmt->bindUint64(":COL1", val);

    stmt->executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1 "
      "FROM "
        "TEST;";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto col1 = rset->columnOptionalUint64("COL1");

    ASSERT_TRUE((bool)col1);

    ASSERT_EQ(val, col1.value());

    ASSERT_FALSE(rset->next());
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, executeCopyInsert_10000_rows) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  ASSERT_TRUE(m_conn->getTableNames().empty());

  // Create a test table
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());
  }

  const size_t nbBulkRows = 10000;
  // Insert a rows into the test table using a bulk method
  {
    PostgresColumn c1("MYCOL1",nbBulkRows);
    PostgresColumn c2("MYCOL2",nbBulkRows);
    PostgresColumn c3("MYCOL3",nbBulkRows);

    for(size_t i=0;i<nbBulkRows;++i) {
      std::string s = "column1 string \" \' \\ \n\r\t for row " + std::to_string(i);
      c1.setFieldValue(i, s);
      uint64_t rval = 123ULL * i;
      s = "column2 string for row " +  std::to_string(i);
      if ((i % 2) == 0) {
        c2.setFieldValue(i, s);
      }
      c3.setFieldValue(i, rval);
    }

    const char *const sql =
      "COPY TEST("
        "COL1,"
        "COL2,"
        "COL3) "
      "FROM STDIN --"
        ":MYCOL1,"
        ":MYCOL2,"
        ":MYCOL3";
    auto stmt = m_conn->createStmt(sql);
    PostgresStmt &pgStmt = dynamic_cast<PostgresStmt &>(*stmt);
    pgStmt.setColumn(c3);
    pgStmt.setColumn(c1);
    pgStmt.setColumn(c2);

    pgStmt.executeCopyInsert(nbBulkRows);
    ASSERT_EQ(nbBulkRows, stmt->getNbAffectedRows());
  }

  {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROWS "
      "FROM "
        "TEST;";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    ASSERT_TRUE(rset->next());

    const auto nbRows = rset->columnOptionalUint64("NB_ROWS");
    ASSERT_TRUE((bool)nbRows);
    ASSERT_EQ((uint64_t)nbBulkRows, nbRows.value());

    ASSERT_FALSE(rset->next());
  }

  {
    const char *const sql =
      "SELECT "
        "COL1 AS COL1,"
        "COL2 AS COL2,"
        "COL3 AS COL3 "
      "FROM "
        "TEST;";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();

    size_t nbrows = 0;
    while(rset->next()) {

      const auto col1 = rset->columnOptionalString("COL1");
      const auto col2 = rset->columnOptionalString("COL2");
      const auto col3 = rset->columnOptionalUint64("COL3");

      ASSERT_TRUE((bool)col1);
      if ((nbrows % 2) == 0) {
        ASSERT_TRUE((bool)col2);
      } else {
        ASSERT_FALSE((bool)col2);
      }
      ASSERT_TRUE((bool)col3);

      std::string s1exp = "column1 string \" \' \\ \n\r\t for row " + std::to_string(nbrows);
      std::string s2exp = "column2 string for row " + std::to_string(nbrows);
      uint64_t rval_exp = 123ULL * nbrows;


      ASSERT_EQ(s1exp.c_str(), col1.value());
      if ((nbrows %2) == 0) {
        ASSERT_EQ(s2exp.c_str(), col2.value());
      }
      ASSERT_EQ(rval_exp, col3.value());

      ++nbrows;
    }
    ASSERT_EQ(nbrows, nbBulkRows);
  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_PostgresStmtTest, nbaffected) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  // Create a test table and insert some rows
  {
    const char *const sql =
      "CREATE TABLE TEST("
        "COL1 VARCHAR(100),"
        "COL2 VARCHAR(100),"
        "COL3 NUMERIC(20,0));";
    auto stmt = m_conn->createStmt(sql);
    stmt->executeNonQuery();
    ASSERT_EQ(1, m_conn->getTableNames().size());
    ASSERT_EQ("TEST", m_conn->getTableNames().front());

    const char *const sql_populate =
      "INSERT INTO TEST(COL1,COL2,COL3) VALUES "
        "('val1',NULL,55),"
        "('val1',NULL,56),"
        "('val2',NULL,56),"
        "('val2','yyy',10),"
        "('val2','yyy',11)";

    m_conn->executeNonQuery(sql_populate);
  }

  // UPDATE and check affected row count
  {
    const char *const sql =
      "UPDATE TEST SET COL1=:NEWVAL WHERE COL1=:OLDVAL";

    auto stmt = m_conn->createStmt(sql);
    stmt->bindString(":NEWVAL", std::string("val3"));
    stmt->bindString(":OLDVAL", std::string("val1"));
    stmt->executeNonQuery();
    ASSERT_EQ(2, stmt->getNbAffectedRows());
  }

  // SELECT and check affected row count
  {
    const char *const sql = "SELECT COL1 FROM TEST WHERE COL1='val2'";
    auto stmt = m_conn->createStmt(sql);
    auto rset = stmt->executeQuery();
    size_t nr=0;
    while(rset->next()) {
      ++nr;
      ASSERT_EQ(nr, stmt->getNbAffectedRows());
    }
    ASSERT_EQ(3, nr);
    ASSERT_EQ(3, stmt->getNbAffectedRows());
  }

}

} // namespace unitTests
