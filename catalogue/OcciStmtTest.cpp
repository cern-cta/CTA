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

#include "catalogue/DbLogin.hpp"
#include "catalogue/OcciEnv.hpp"
#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciRset.hpp"
#include "catalogue/DbStmt.hpp"
#include "tests/OraUnitTestsCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_DbStmtTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_DbStmtTest, executeQuery) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::parseFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT * FROM DUAL";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
}

} // namespace unitTests
