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
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_catalogue_DbLoginTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_DbLoginTest, constructor) {
  using namespace cta::catalogue;

  const DbLogin inMemoryLogin(DbLogin::DBTYPE_IN_MEMORY, "", "", "");
  ASSERT_EQ(DbLogin::DBTYPE_IN_MEMORY, inMemoryLogin.dbType);
  ASSERT_TRUE(inMemoryLogin.username.empty());
  ASSERT_TRUE(inMemoryLogin.password.empty());
  ASSERT_TRUE(inMemoryLogin.database.empty());

  const DbLogin oracleLogin(DbLogin::DBTYPE_ORACLE, "username", "password", "database");
  ASSERT_EQ(DbLogin::DBTYPE_ORACLE, oracleLogin.dbType);
  ASSERT_EQ(std::string("username"), oracleLogin.username);
  ASSERT_EQ(std::string("password"), oracleLogin.password);
  ASSERT_EQ(std::string("database"), oracleLogin.database);

  const DbLogin sqliteLogin(DbLogin::DBTYPE_SQLITE, "", "", "filename");
  ASSERT_EQ(DbLogin::DBTYPE_SQLITE, sqliteLogin.dbType);
  ASSERT_TRUE(sqliteLogin.username.empty());
  ASSERT_TRUE(sqliteLogin.password.empty());
  ASSERT_EQ(std::string("filename"), sqliteLogin.database);
}

TEST_F(cta_catalogue_DbLoginTest, parseStream_in_memory) {
  using namespace cta::catalogue;

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

  const DbLogin dbLogin = DbLogin::parseStream(inputStream);
  ASSERT_EQ(DbLogin::DBTYPE_IN_MEMORY, dbLogin.dbType);
  ASSERT_TRUE(dbLogin.username.empty());
  ASSERT_TRUE(dbLogin.password.empty());
  ASSERT_TRUE(dbLogin.database.empty());
}

TEST_F(cta_catalogue_DbLoginTest, parseStream_oracle) {
  using namespace cta::catalogue;

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

  const DbLogin dbLogin = DbLogin::parseStream(inputStream);
  ASSERT_EQ(DbLogin::DBTYPE_ORACLE, dbLogin.dbType);
  ASSERT_EQ(std::string("username"), dbLogin.username);
  ASSERT_EQ(std::string("password"), dbLogin.password);
  ASSERT_EQ(std::string("database"), dbLogin.database);
}

TEST_F(cta_catalogue_DbLoginTest, parseStream_sqlite) {
  using namespace cta::catalogue;

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

  const DbLogin dbLogin = DbLogin::parseStream(inputStream);
  ASSERT_EQ(DbLogin::DBTYPE_SQLITE, dbLogin.dbType);
  ASSERT_TRUE(dbLogin.username.empty());
  ASSERT_TRUE(dbLogin.password.empty());
  ASSERT_EQ(std::string("filename"), dbLogin.database);
}

TEST_F(cta_catalogue_DbLoginTest, parseStream_invalid) {
  using namespace cta;
  using namespace cta::catalogue;

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

  ASSERT_THROW(DbLogin::parseStream(inputStream), exception::Exception);
}

} // namespace unitTests
