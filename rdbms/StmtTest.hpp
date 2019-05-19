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
