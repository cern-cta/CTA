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

#include <cstring>
#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_OcciRsetTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_OcciRsetTest, executeQuery) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::readFromFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT DUMMY FROM DUAL";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
  ASSERT_TRUE(rset->next());
  std::string text(rset->columnText("DUMMY"));
  ASSERT_EQ(std::string("X"), text);
  ASSERT_FALSE(rset->next());
}

TEST_F(cta_catalogue_OcciRsetTest, executeQueryRelyOnRsetDestructorForCacheDelete) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::readFromFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT DUMMY FROM DUAL";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
  ASSERT_TRUE(rset->next());
  std::string text(rset->columnText("DUMMY"));
  ASSERT_EQ(std::string("X"), text);
}

TEST_F(cta_catalogue_OcciRsetTest, executeQuery_uint32_t) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::readFromFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT 1234 AS I FROM DUAL";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
  ASSERT_TRUE(rset->next());
  const uint32_t i = rset->columnUint64("I");
  ASSERT_EQ(1234, i);
  ASSERT_FALSE(rset->next());
}

// TODO - Implement 64-bit int executeQuery test because the current code will fail

TEST_F(cta_catalogue_OcciRsetTest, bind_c_string) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::readFromFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT DUMMY FROM DUAL WHERE DUMMY = :DUMMY";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  stmt->bindString(":DUMMY", "X");
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
  ASSERT_TRUE(rset->next());
  std::string text(rset->columnText("DUMMY"));
  ASSERT_EQ(std::string("X"), text);
  ASSERT_FALSE(rset->next());
}

TEST_F(cta_catalogue_OcciRsetTest, bind_uint32_t) {
  using namespace cta;
  using namespace cta::catalogue;

  const DbLogin dbLogin = DbLogin::readFromFile(g_cmdLineArgs.oraDbConnFile);
  OcciEnv env;
  std::unique_ptr<DbConn> conn(env.createConn(
    dbLogin.username.c_str(),
    dbLogin.password.c_str(),
    dbLogin.database.c_str()));
  const char *const sql = "SELECT :N AS AN_UNSIGNED_INT FROM DUAL";
  std::unique_ptr<DbStmt> stmt(conn->createStmt(sql));
  stmt->bindUint64(":N", 1234);
  std::unique_ptr<DbRset> rset(stmt->executeQuery());
  ASSERT_TRUE(rset->next());
  const uint32_t n = rset->columnUint64("AN_UNSIGNED_INT");
  ASSERT_EQ(1234, n);
  ASSERT_FALSE(rset->next());
}

// TODO - Implement 64-bit int bind test because the current code will fail

} // namespace unitTests
