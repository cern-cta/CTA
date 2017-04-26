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

#include "Login.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_LoginTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_LoginTest, constructor) {
  using namespace cta::rdbms;

  const Login inMemoryLogin(Login::DBTYPE_IN_MEMORY, "", "", "");
  ASSERT_EQ(Login::DBTYPE_IN_MEMORY, inMemoryLogin.dbType);
  ASSERT_TRUE(inMemoryLogin.username.empty());
  ASSERT_TRUE(inMemoryLogin.password.empty());
  ASSERT_TRUE(inMemoryLogin.database.empty());

  const Login oracleLogin(Login::DBTYPE_ORACLE, "username", "password", "database");
  ASSERT_EQ(Login::DBTYPE_ORACLE, oracleLogin.dbType);
  ASSERT_EQ(std::string("username"), oracleLogin.username);
  ASSERT_EQ(std::string("password"), oracleLogin.password);
  ASSERT_EQ(std::string("database"), oracleLogin.database);

  const Login sqliteLogin(Login::DBTYPE_SQLITE, "", "", "filename");
  ASSERT_EQ(Login::DBTYPE_SQLITE, sqliteLogin.dbType);
  ASSERT_TRUE(sqliteLogin.username.empty());
  ASSERT_TRUE(sqliteLogin.password.empty());
  ASSERT_EQ(std::string("filename"), sqliteLogin.database);
}

TEST_F(cta_rdbms_LoginTest, parseStream_in_memory) {
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "in_memory";
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  const Login login = Login::parseStream(inputStream);
  ASSERT_EQ(Login::DBTYPE_IN_MEMORY, login.dbType);
  ASSERT_TRUE(login.username.empty());
  ASSERT_TRUE(login.password.empty());
  ASSERT_TRUE(login.database.empty());
}

TEST_F(cta_rdbms_LoginTest, parseStream_oracle) {
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "oracle:username/password@database" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  const Login login = Login::parseStream(inputStream);
  ASSERT_EQ(Login::DBTYPE_ORACLE, login.dbType);
  ASSERT_EQ(std::string("username"), login.username);
  ASSERT_EQ(std::string("password"), login.password);
  ASSERT_EQ(std::string("database"), login.database);
}

TEST_F(cta_rdbms_LoginTest, parseStream_oracle_password_with_a_hash) {
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "oracle:username/password_with_a_hash#@database" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  const Login login = Login::parseStream(inputStream);
  ASSERT_EQ(Login::DBTYPE_ORACLE, login.dbType);
  ASSERT_EQ(std::string("username"), login.username);
  ASSERT_EQ(std::string("password"), login.password);
  ASSERT_EQ(std::string("database"), login.database);
}

TEST_F(cta_rdbms_LoginTest, parseStream_sqlite) {
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "sqlite:filename" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  const Login login = Login::parseStream(inputStream);
  ASSERT_EQ(Login::DBTYPE_SQLITE, login.dbType);
  ASSERT_TRUE(login.username.empty());
  ASSERT_TRUE(login.password.empty());
  ASSERT_EQ(std::string("filename"), login.database);
}

TEST_F(cta_rdbms_LoginTest, parseStream_invalid) {
  using namespace cta;
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "invalid_connection_string";
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  ASSERT_THROW(Login::parseStream(inputStream), exception::Exception);
}

} // namespace unitTests
