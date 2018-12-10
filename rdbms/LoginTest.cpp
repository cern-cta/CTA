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

#include "common/exception/Exception.hpp"
#include "rdbms/Login.hpp"

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

  const Login inMemoryLogin(Login::DBTYPE_IN_MEMORY, "", "", "", "", 0);
  ASSERT_EQ(Login::DBTYPE_IN_MEMORY, inMemoryLogin.dbType);
  ASSERT_TRUE(inMemoryLogin.username.empty());
  ASSERT_TRUE(inMemoryLogin.password.empty());
  ASSERT_TRUE(inMemoryLogin.database.empty());

  const Login oracleLogin(Login::DBTYPE_ORACLE, "username", "password", "database", "", 0);
  ASSERT_EQ(Login::DBTYPE_ORACLE, oracleLogin.dbType);
  ASSERT_EQ(std::string("username"), oracleLogin.username);
  ASSERT_EQ(std::string("password"), oracleLogin.password);
  ASSERT_EQ(std::string("database"), oracleLogin.database);

  const Login sqliteLogin(Login::DBTYPE_SQLITE, "", "", "filename", "", 0);
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

TEST_F(cta_rdbms_LoginTest, parseStream_invalid_in_memory) {
  using namespace cta;
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "in_memory:invalid";
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  ASSERT_THROW(Login::parseStream(inputStream), exception::Exception);
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

TEST_F(cta_rdbms_LoginTest, parseStream_invalid_oracle) {
  using namespace cta;
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "oracle:invalid" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  ASSERT_THROW(Login::parseStream(inputStream), exception::Exception);
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
  ASSERT_EQ(std::string("password_with_a_hash#"), login.password);
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

TEST_F(cta_rdbms_LoginTest, parseStream_invalid_sqlite) {
  using namespace cta;
  using namespace cta::rdbms;

  std::stringstream inputStream;
  inputStream << "# A comment" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << "# Another comment" << std::endl;
  inputStream << "sqlite:" << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;
  inputStream << std::endl;

  ASSERT_THROW(Login::parseStream(inputStream), exception::Exception);
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

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_good_day) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "dbType:connectionDetails";
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_EQ("dbType", typeAndDetails.dbTypeStr);
  ASSERT_EQ("connectionDetails", typeAndDetails.connectionDetails);
}

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_non_empty_string_no_colon) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "dbType";
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_EQ("dbType", typeAndDetails.dbTypeStr);
  ASSERT_TRUE(typeAndDetails.connectionDetails.empty());
}

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_empty_string) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString;
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_TRUE(typeAndDetails.dbTypeStr.empty());
  ASSERT_TRUE(typeAndDetails.connectionDetails.empty());
}

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_non_empty_string_colon_is_last) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "dbType:";
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_EQ("dbType", typeAndDetails.dbTypeStr);
  ASSERT_TRUE(typeAndDetails.connectionDetails.empty());
}

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_just_a_colon) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = ":";
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_TRUE(typeAndDetails.dbTypeStr.empty());
  ASSERT_TRUE(typeAndDetails.connectionDetails.empty());
}

TEST_F(cta_rdbms_LoginTest, parseDbTypeAndConnectionDetails_many_colons) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = ":::::";
  const auto typeAndDetails = Login::parseDbTypeAndConnectionDetails(connectionString);

  ASSERT_TRUE(typeAndDetails.dbTypeStr.empty());
  ASSERT_EQ("::::", typeAndDetails.connectionDetails);
}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_fullurl) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://user:pass@hostname:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("user", login.username);
  ASSERT_EQ("pass", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_fullurl_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://user:pass@[::1]:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("user", login.username);
  ASSERT_EQ("pass", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_shortest_url) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://hostname/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_shortest_url_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://[::1]/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_userpass_with_seq) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://@hostname:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_userpass_with_seq_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://@[::1]:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_userpass_no_seq) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://hostname:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_userpass_no_seq_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://[::1]:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_pass) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://username@hostname:3306/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("username", login.username);
  ASSERT_EQ("", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_port_with_seq) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://username:password@hostname:/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("username", login.username);
  ASSERT_EQ("password", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_port_with_seq_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://username:password@[::1]:/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("username", login.username);
  ASSERT_EQ("password", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_port_no_seq) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://username:password@hostname/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("username", login.username);
  ASSERT_EQ("password", login.password);
  ASSERT_EQ("hostname", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_partialurl_no_port_no_seq_ipv6) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://username:password@[::1]/dbname";
  const auto login = Login::parseString(connectionString);

  ASSERT_EQ(Login::DBTYPE_MYSQL, login.dbType);
  ASSERT_EQ("username", login.username);
  ASSERT_EQ("password", login.password);
  ASSERT_EQ("[::1]", login.hostname);
  ASSERT_EQ(3306, login.port);
  ASSERT_EQ("dbname", login.database);

}

// invalid mysql config
TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql:";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty2) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty3) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql:/";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}


TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty_db) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://hostname";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty_db_with_sep) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql://hostname/";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}

TEST_F(cta_rdbms_LoginTest, parseString_MySql_invalid_empty_host) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string connectionString = "mysql:///dbname";
  ASSERT_THROW(Login::parseString(connectionString), exception::Exception);
}

} // namespace unitTests
