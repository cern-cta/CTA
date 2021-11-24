/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/CheckConstraintError.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/PrimaryKeyError.hpp"
#include "rdbms/StmtTest.hpp"
#include "rdbms/UniqueError.hpp"

#include <gtest/gtest.h>

namespace unitTests {

//------------------------------------------------------------------------------
// Setup
//------------------------------------------------------------------------------
void cta_rdbms_StmtTest::SetUp() {
  using namespace cta::rdbms;

  if(!m_connPool) {
    m_login = GetParam()->create();
    const uint64_t maxNbConns = 1;
    m_connPool = cta::make_unique<ConnPool>(m_login, maxNbConns);
  }

  m_conn = m_connPool->getConn();
  ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, m_conn.getAutocommitMode());

  try {
    m_conn.executeNonQuery("DROP TABLE STMT_TEST");
  } catch(...) {
    // Do nothing
  }

  const std::string createTableSql = getCreateStmtTestTableSql();
  m_conn.executeNonQuery(createTableSql);
}

//------------------------------------------------------------------------------
// getCreateStmtTestTableSql
//------------------------------------------------------------------------------
std::string cta_rdbms_StmtTest::getCreateStmtTestTableSql() {
  using namespace cta;
  using namespace cta::rdbms;

  try {
    std::string sql =
      "CREATE TABLE STMT_TEST("                                      "\n"
      "  ID         UINT64TYPE CONSTRAINT STMT_TEST_ID_NN NOT NULL," "\n"
      "  DOUBLE_COL FLOAT,"                                          "\n"
      "  UINT8_COL  UINT8TYPE,"                                      "\n"
      "  UINT16_COL UINT16TYPE,"                                     "\n"
      "  UINT32_COL UINT32TYPE,"                                     "\n"
      "  UINT64_COL UINT64TYPE,"                                     "\n"
      "  STRING_COL VARCHAR(100),"                                   "\n"
      "  BLOB_COL   BLOBTYPE,"                                      "\n"
      "  BOOL_COL   CHAR(1) DEFAULT '0',"                            "\n"
      "  CONSTRAINT STMT_TEST_PK PRIMARY KEY(ID),"                   "\n"
      "  CONSTRAINT BOOL_COL_BOOL_CK CHECK(BOOL_COL IN ('0', '1'))"  "\n"
      ")";

    switch(m_login.dbType) {
    case Login::DBTYPE_IN_MEMORY:
      break;
    case Login::DBTYPE_ORACLE:
      utils::searchAndReplace(sql, "UINT8TYPE", "NUMERIC(3, 0)");
      utils::searchAndReplace(sql, "UINT16TYPE", "NUMERIC(5, 0)");
      utils::searchAndReplace(sql, "UINT32TYPE", "NUMERIC(10, 0)");
      utils::searchAndReplace(sql, "UINT64TYPE", "NUMERIC(20, 0)");
      utils::searchAndReplace(sql, "VARCHAR", "VARCHAR2");
      utils::searchAndReplace(sql, "BLOBTYPE", "RAW(200)");
      break;
    case Login::DBTYPE_SQLITE:
      utils::searchAndReplace(sql, "UINT8TYPE", "INTEGER");
      utils::searchAndReplace(sql, "UINT16TYPE", "INTEGER");
      utils::searchAndReplace(sql, "UINT32TYPE", "INTEGER");
      utils::searchAndReplace(sql, "UINT64TYPE", "INTEGER");
      utils::searchAndReplace(sql, "BLOBTYPE", "BLOB(200)");
      break;
    case Login::DBTYPE_POSTGRESQL:
      utils::searchAndReplace(sql, "UINT8TYPE", "NUMERIC(3, 0)");
      utils::searchAndReplace(sql, "UINT16TYPE", "NUMERIC(5, 0)");
      utils::searchAndReplace(sql, "UINT32TYPE", "NUMERIC(10, 0)");
      utils::searchAndReplace(sql, "UINT64TYPE", "NUMERIC(20, 0)");
      utils::searchAndReplace(sql, "BLOBTYPE", "BYTEA");
      break;
    case Login::DBTYPE_NONE:
      {
        throw exception::Exception("Cannot create SQL for database type DBTYPE_NONE");
      }
    default:
      {
        std::ostringstream msg;
        msg << "Unknown database type: intVal=" << m_login.dbType;
        throw exception::Exception(msg.str());
      }
    }

    return sql;
  } catch(exception::Exception &ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " failed: " << ex.getMessage().str();
    ex.getMessage().str(msg.str());
    throw ex;
  }
}

//------------------------------------------------------------------------------
// TearDown
//------------------------------------------------------------------------------
void cta_rdbms_StmtTest::TearDown() {
  using namespace cta::rdbms;
  try {
    m_conn.executeNonQuery("DROP TABLE STMT_TEST");
  } catch(...) {
    // Do nothing
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindDouble) {
  using namespace cta::rdbms;

  const double insertValue = 1.234;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  DOUBLE_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :DOUBLE_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindDouble(":DOUBLE_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  DOUBLE_COL AS DOUBLE_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    {
      const auto selectValue = rset.columnOptionalDouble("DOUBLE_COL");

      ASSERT_TRUE((bool)selectValue);

      const double diff = insertValue - selectValue.value();
      ASSERT_TRUE(0.000001 > diff);
    }

    {
      const auto selectValue = rset.columnDouble("DOUBLE_COL");

      const double diff = insertValue - selectValue;
      ASSERT_TRUE(0.000001 > diff);
    }

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindBlob) {
  using namespace cta::rdbms;

  const std::string insertBlob = "\x00\x01\x02\x03\n\t\t\0\0\0\0";

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  BLOB_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :BLOB_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindBlob(":BLOB_COL", insertBlob);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  BLOB_COL AS BLOB_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    {
      const auto selectValue = rset.columnBlob("BLOB_COL");

      ASSERT_EQ(insertBlob, selectValue);
    }

    ASSERT_FALSE(rset.next());
  }
}


TEST_P(cta_rdbms_StmtTest, insert_with_bindDouble_null) {
  using namespace cta::rdbms;

  const cta::optional<double> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  DOUBLE_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :DOUBLE_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindDouble(":DOUBLE_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  DOUBLE_COL AS DOUBLE_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalDouble("DOUBLE_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnDouble("DOUBLE_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint8) {
  using namespace cta::rdbms;

  const uint8_t insertValue = 123;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT8_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT8_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint8(":UINT8_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                   "\n"
      "  UINT8_COL AS UINT8_COL" "\n"
      "FROM"                     "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    {
      const auto selectValue = rset.columnOptionalUint8("UINT8_COL");

      ASSERT_TRUE((bool)selectValue);

      ASSERT_EQ(insertValue, selectValue.value());
    }

    ASSERT_EQ(insertValue, rset.columnUint8("UINT8_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint8_2_pow_8_minus_1) {
  using namespace cta::rdbms;

  const uint8_t insertValue = 255U;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT8_COL)"           "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT8_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint8(":UINT8_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                   "\n"
      "  UINT8_COL AS UINT8_COL" "\n"
      "FROM"                     "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    {
      const auto selectValue = rset.columnOptionalUint8("UINT8_COL");

      ASSERT_TRUE((bool)selectValue);

      ASSERT_EQ(insertValue, selectValue.value());
    }

    ASSERT_EQ(insertValue, rset.columnUint8("UINT8_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint8_null) {
  using namespace cta::rdbms;

  const cta::optional<uint8_t> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT8_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT8_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint8(":UINT8_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT8_COL AS UINT8_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint8("UINT8_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnUint8("UINT8_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}


TEST_P(cta_rdbms_StmtTest, insert_with_bindUint16) {
  using namespace cta::rdbms;

  const uint16_t insertValue = 1234;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT16_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT16_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint16(":UINT16_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT16_COL AS UINT16_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint16("UINT16_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint64("UINT16_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint16_2_pow_16_minus_1) {
  using namespace cta::rdbms;

  const uint16_t insertValue = 65535U;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT16_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT16_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint16(":UINT16_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT16_COL AS UINT16_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint16("UINT16_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint16("UINT16_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint16_null) {
  using namespace cta::rdbms;

  const cta::optional<uint16_t> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT16_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT16_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint16(":UINT16_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT16_COL AS UINT16_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint16("UINT16_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnUint16("UINT16_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint32) {
  using namespace cta::rdbms;

  const uint32_t insertValue = 1234;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT32_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT32_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint32(":UINT32_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT32_COL AS UINT32_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint32("UINT32_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint32("UINT32_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint32_2_pow_32_minus_1) {
  using namespace cta::rdbms;

  const uint32_t insertValue = 4294967295U;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT32_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT32_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint32(":UINT32_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT32_COL AS UINT32_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint32("UINT32_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());
    ASSERT_EQ(insertValue, rset.columnUint32("UINT32_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint32_null) {
  using namespace cta::rdbms;

  const cta::optional<uint32_t> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT32_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT32_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint32(":UINT32_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT32_COL AS UINT32_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint32("UINT32_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnUint32("UINT32_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint64) {
  using namespace cta::rdbms;

  const uint64_t insertValue = 1234;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT64_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT64_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":UINT64_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT64_COL AS UINT64_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint64("UINT64_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint64("UINT64_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint64_2_pow_64_minus_1) {
  using namespace cta::rdbms;

  const uint64_t insertValue = 18446744073709551615U;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT64_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT64_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":UINT64_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT64_COL AS UINT64_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint64("UINT64_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint64("UINT64_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindUint64_2_pow_64_minus_2) {
  using namespace cta::rdbms;

  const uint64_t insertValue = 18446744073709551614U;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  UINT64_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :UINT64_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":UINT64_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  UINT64_COL AS UINT64_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint64("UINT64_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint64("UINT64_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindString_null) {
  using namespace cta::rdbms;

  const cta::optional<std::string> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  STRING_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :STRING_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindString(":STRING_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  STRING_COL AS STRING_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalString("STRING_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnString("STRING_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindString) {
  using namespace cta::rdbms;

  const std::string insertValue = "value";

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  STRING_COL)"          "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :STRING_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindString(":STRING_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  STRING_COL AS STRING_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalString("STRING_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnString("STRING_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindBool_null) {
  using namespace cta::rdbms;

  const cta::optional<bool> insertValue; // Null value

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  BOOL_COL) "         "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :BOOL_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindBool(":BOOL_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                     "\n"
      "  BOOL_COL AS BOOL_COL" "\n"
      "FROM"                       "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalBool("BOOL_COL");

    ASSERT_FALSE((bool)selectValue);

    ASSERT_THROW(rset.columnBool("BOOL_COL"), NullDbValue);

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindBool_true) {
  using namespace cta::rdbms;

  const bool insertValue = true;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  BOOL_COL)"            "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :BOOL_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindBool(":BOOL_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                 "\n"
      "  BOOL_COL AS BOOL_COL" "\n"
      "FROM"                   "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalBool("BOOL_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnBool("BOOL_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindBool_false) {
  using namespace cta::rdbms;

  const bool insertValue = false;

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
      "  BOOL_COL)"            "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
      "  :BOOL_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindBool(":BOOL_COL", insertValue);
    stmt.executeNonQuery();
  }

  // Select the row back from the table
  {
    const char *const sql =
      "SELECT"                 "\n"
      "  BOOL_COL AS BOOL_COL" "\n"
      "FROM"                   "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalBool("BOOL_COL");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnBool("BOOL_COL"));

    ASSERT_FALSE(rset.next());
  }
}

TEST_P(cta_rdbms_StmtTest, insert_with_bindString_invalid_bool_value) {
  using namespace cta::rdbms;

  const std::string insertValue = "2"; // null, "0" and "1" are valid values

  // Insert a row into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID,"                  "\n"
        "BOOL_COL)"            "\n"
      "VALUES("                "\n"
      "  1,"                   "\n"
        ":BOOL_COL)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindString(":BOOL_COL", insertValue);

    ASSERT_THROW(stmt.executeNonQuery(), CheckConstraintError);
  }
}

TEST_P(cta_rdbms_StmtTest, insert_same_primary_twice) {
  using namespace cta::rdbms;

  const uint64_t insertValue = 1234;

  // Insert an ID into the test table
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID)"                  "\n"
      "VALUES("                "\n"
      "  :ID)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":ID", insertValue);
    stmt.executeNonQuery();
  }

  // Select the ID back from the table
  {
    const char *const sql =
      "SELECT"      "\n"
      "  ID AS ID"  "\n"
      "FROM"        "\n"
      "  STMT_TEST";
    auto stmt = m_conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());

    const auto selectValue = rset.columnOptionalUint64("ID");

    ASSERT_TRUE((bool)selectValue);

    ASSERT_EQ(insertValue, selectValue.value());

    ASSERT_EQ(insertValue, rset.columnUint64("ID"));

    ASSERT_FALSE(rset.next());
  }

  // Attempt to insert the same ID again
  {
    const char *const sql =
      "INSERT INTO STMT_TEST(" "\n"
      "  ID) "                 "\n"
      "VALUES("                "\n"
      "  :ID)";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindUint64(":ID", insertValue);
    switch(m_login.dbType) {
    case Login::DBTYPE_IN_MEMORY:
    case Login::DBTYPE_ORACLE:
    case Login::DBTYPE_POSTGRESQL:
      ASSERT_THROW(stmt.executeNonQuery(), UniqueError);
      break;
    default:
      ASSERT_THROW(stmt.executeNonQuery(), PrimaryKeyError);
    }
  }
}

} // namespace unitTests
