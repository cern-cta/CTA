/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/LoginFactory.hpp"
#include "rdbms/Conn.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_rdbms_StmtTest : public ::testing::TestWithParam<cta::rdbms::LoginFactory*> {
protected:
  /**
   * The database login.
   */
  cta::rdbms::Login m_login;

  /**
   * The database connection pool.
   */
  std::unique_ptr<cta::rdbms::ConnPool> m_connPool;

  /**
   * The database connection.
   */
  cta::rdbms::Conn m_conn;

  virtual void SetUp();

  /**
   * Returns the SQL to create the STMT_TEST table taking into account any
   * differences between the various types of database backend.
   *
   * @return The SQL to create the STMT_TEST table taking into account any
   * differences between the various types of database backend.
   */
  std::string getCreateStmtTestTableSql();

  virtual void TearDown();

}; // cta_rdbms_StmtTest

} // namespace unitTests
