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
#include "rdbms/ConnTest.hpp"
#include "rdbms/Login.hpp"

#include <gtest/gtest.h>

namespace unitTests {

//------------------------------------------------------------------------------
// Setup
//------------------------------------------------------------------------------
void cta_rdbms_ConnTest::SetUp() {
  m_login = GetParam()->create();
}

//------------------------------------------------------------------------------
// TearDown
//------------------------------------------------------------------------------
void cta_rdbms_ConnTest::TearDown() {}

TEST_P(cta_rdbms_ConnTest, getAutocommitMode_default_AUTOCOMMIT_ON) {
  using namespace cta::rdbms;

  const uint64_t maxNbConns = 1;
  ConnPool connPool(m_login, maxNbConns);
  auto conn = connPool.getConn();

  ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
}

TEST_P(cta_rdbms_ConnTest, setAutocommitMode_AUTOCOMMIT_ON) {
  using namespace cta::rdbms;

  const uint64_t maxNbConns = 1;
  ConnPool connPool(m_login, maxNbConns);
  auto conn = connPool.getConn();

  ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());

  conn.setAutocommitMode(AutocommitMode::AUTOCOMMIT_ON);
  ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
}

TEST_P(cta_rdbms_ConnTest, setAutocommitMode_AUTOCOMMIT_OFF) {
  using namespace cta::rdbms;

  const uint64_t maxNbConns = 1;
  ConnPool connPool(m_login, maxNbConns);
  auto conn = connPool.getConn();
  ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());

  switch (m_login.dbType) {
    case Login::DBTYPE_ORACLE:
      conn.setAutocommitMode(AutocommitMode::AUTOCOMMIT_OFF);
      ASSERT_EQ(AutocommitMode::AUTOCOMMIT_OFF, conn.getAutocommitMode());
      break;
    case Login::DBTYPE_IN_MEMORY:
    case Login::DBTYPE_SQLITE:
    case Login::DBTYPE_POSTGRESQL:
      ASSERT_THROW(conn.setAutocommitMode(AutocommitMode::AUTOCOMMIT_OFF), Conn::AutocommitModeNotSupported);
      break;
    case Login::DBTYPE_NONE:
      FAIL() << "Unexpected database login type: value=DBTYPE_NONE";
      break;
    default:
      FAIL() << "Unknown database login type: intValue=" << m_login.dbType;
  }
}

TEST_P(cta_rdbms_ConnTest, loan_return_loan_conn_setAutocommitMode_AUTOCOMMIT_OFF) {
  using namespace cta::rdbms;

  const uint64_t maxNbConn = 1;
  ConnPool connPool(m_login, maxNbConn);

  switch (m_login.dbType) {
    case Login::DBTYPE_ORACLE: {
      auto conn = connPool.getConn();
      ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
      conn.setAutocommitMode(AutocommitMode::AUTOCOMMIT_OFF);
      ASSERT_EQ(AutocommitMode::AUTOCOMMIT_OFF, conn.getAutocommitMode());
    }
      {
        auto conn = connPool.getConn();
        ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
      }
      break;
    case Login::DBTYPE_IN_MEMORY:
    case Login::DBTYPE_SQLITE:
    case Login::DBTYPE_POSTGRESQL: {
      auto conn = connPool.getConn();
      ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
      ASSERT_THROW(conn.setAutocommitMode(AutocommitMode::AUTOCOMMIT_OFF), Conn::AutocommitModeNotSupported);
    }
      {
        auto conn = connPool.getConn();
        ASSERT_EQ(AutocommitMode::AUTOCOMMIT_ON, conn.getAutocommitMode());
      }
      break;
    case Login::DBTYPE_NONE:
      FAIL() << "Unexpected database login type: value=DBTYPE_NONE";
      break;
    default:
      FAIL() << "Unknown database login type: intValue=" << m_login.dbType;
  }
}

TEST_P(cta_rdbms_ConnTest, createTableInMemoryDatabase_executeNonQuery) {
  using namespace cta::rdbms;

  const std::string sql = "CREATE TABLE CONN_TEST(ID INTEGER)";

  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    const uint64_t maxNbConns = 1;
    ConnPool connPool(login, maxNbConns);
    auto conn = connPool.getConn();

    ASSERT_TRUE(conn.getTableNames().empty());

    conn.executeNonQuery(sql);

    ASSERT_EQ(1, conn.getTableNames().size());
  }
}

TEST_P(cta_rdbms_ConnTest, createSameTableInTwoSeparateInMemoryDatabases_executeNonQuery) {
  using namespace cta::rdbms;

  const std::string sql = "CREATE TABLE CONN_TEST(ID INTEGER)";

  // First in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    const uint64_t maxNbConns = 1;
    ConnPool connPool(login, maxNbConns);
    auto conn = connPool.getConn();

    ASSERT_TRUE(conn.getTableNames().empty());

    conn.executeNonQuery(sql);

    ASSERT_EQ(1, conn.getTableNames().size());
  }

  // Second in-memory database
  {
    const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
    const uint64_t maxNbConns = 1;
    ConnPool connPool(login, maxNbConns);
    auto conn = connPool.getConn();

    ASSERT_TRUE(conn.getTableNames().empty());

    conn.executeNonQuery(sql);

    ASSERT_EQ(1, conn.getTableNames().size());
  }
}

}  // namespace unitTests
