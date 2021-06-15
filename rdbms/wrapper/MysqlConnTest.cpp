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

#include "common/exception/DatabaseConstraintError.hpp"
#include "common/exception/DatabasePrimaryKeyError.hpp"

#include "rdbms/Login.hpp"

#include "rdbms/wrapper/MysqlConn.hpp"
#include "rdbms/wrapper/MysqlConnFactory.hpp"
#include "rdbms/wrapper/MysqlStmt.hpp"

#include "rdbms/wrapper/ConnFactoryFactory.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <algorithm>

namespace unitTests {

/**
 *  To enable the test case, just set environment variable GTEST_FILTER 
 *  and GTEST_ALSO_RUN_DISABLED_TESTS
 *
 * $ export GTEST_ALSO_RUN_DISABLED_TESTS=1
 * $ export GTEST_FILTER=*Mysql* 
 * $ ./tests/cta-unitTests
 */

class DISABLED_cta_rdbms_wrapper_MysqlConnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_conn_success) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  MysqlConn conn("localhost", "cta", "ctatest", "testdb", 3306);
  ASSERT_TRUE(conn.isOpen());

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_conn_failed_wrong_passwd) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  std::unique_ptr<MysqlConn> conn;
  ASSERT_THROW(conn.reset(new MysqlConn("localhost", "cta", "wrongctatest", "testdb", 3306)), exception::Exception);
  ASSERT_EQ(conn, nullptr);
}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_connfactory_success) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_connfactory_failed_wrong_passwd) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "wrongctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  ASSERT_THROW(conn = conn_factory.create(), exception::Exception);
  ASSERT_EQ(conn, nullptr);

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_connfactoryfactory_success) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  const std::string connectionString = "mysql://cta:ctatest@localhost:3306/testdb";
  const auto login = Login::parseString(connectionString);

  std::unique_ptr<ConnFactory> conn_factory;
  conn_factory = ConnFactoryFactory::create(login);

  std::unique_ptr<Conn> conn;
  conn = conn_factory->create();

  ASSERT_TRUE(conn->isOpen());

}


TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_stmt) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  // Example:
  // https://dev.mysql.com/doc/refman/5.6/en/mysql-stmt-execute.html
  const std::string sql_drop_table = "DROP TABLE IF EXISTS test_table";
  stmt = conn->createStmt(sql_drop_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_table = "CREATE TABLE test_table(col1 INT, col2 VARCHAR(40), col3 SMALLINT)";
  stmt = conn->createStmt(sql_create_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_str = "insert into test_table(col1,col2,col3) values(?,?,?)";
  stmt = conn->createStmt(sql_str);

  // stmt->executeNonQuery();
  conn->commit();
}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_stmt_named_binding) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  // Example:
  // https://dev.mysql.com/doc/refman/5.6/en/mysql-stmt-execute.html
  const std::string sql_drop_table = "DROP TABLE IF EXISTS test_table";
  stmt = conn->createStmt(sql_drop_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_table = "CREATE TABLE test_table(col1 INT, col2 VARCHAR(40), col3 SMALLINT)";
  stmt = conn->createStmt(sql_create_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_str = "insert into test_table(col1,col2,col3) values(:col1,:col2,:col3)";
  const std::string sql_real = "insert into test_table(col1,col2,col3) values(?,?,?)";
  const std::string sql_transit = Mysql::translate_it(sql_str);
  ASSERT_EQ(sql_real, sql_transit);

  stmt = conn->createStmt(sql_str);
  stmt->bindUint64(":col1", 1);
  stmt->bindString(":col2", "2");
  stmt->bindUint64(":col3", 3);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_OFF);

  stmt->bindUint64(":col1", 1);
  stmt->bindString(":col2", "The most popular Open Source database");
  stmt->bindUint64(":col3", 3);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_OFF);


  stmt->bindString(":col2", "The most popular Open Source database again");
  stmt->bindUint64(":col3", 33);
  stmt->bindUint64(":col1", 11);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_OFF);

  // try to test with null
  stmt->bindUint64(":col1", 1);
  stmt->bindString(":col2", optional<std::string>());
  // stmt->bindString(":col2", nullopt);
  stmt->bindUint64(":col3", 3);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_OFF);

  stmt->bindUint64(":col1", nullopt);
  stmt->bindString(":col2", optional<std::string>());
  // stmt->bindString(":col2", nullopt);
  stmt->bindUint64(":col3", 3);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_OFF);


  conn->commit();

  // Start Query
  const std::string sql_query = "select col1,col2 from test_table";
  stmt = conn->createStmt(sql_query);

  std::unique_ptr<Rset> rset = stmt->executeQuery(AutocommitMode::AUTOCOMMIT_OFF);
  ASSERT_NE(rset, nullptr);

  // retrieve the first row
  ASSERT_TRUE(rset->next());

  const auto col1 = rset->columnOptionalUint64("col1");
  const auto col2 = rset->columnOptionalString("col2");

  ASSERT_EQ(col1.value(), 1);
  ASSERT_EQ(col2.value(), "2");

  // retrieve the second row
  ASSERT_TRUE(rset->next());

  const auto col1_row2 = rset->columnOptionalUint64("col1");
  const auto col2_row2 = rset->columnOptionalString("col2");

  ASSERT_EQ(col1_row2.value(), 1);
  ASSERT_EQ(col2_row2.value(), "The most popular Open Source database");

  // retrieve the third row (truncated)
  ASSERT_TRUE(rset->next());

  const auto col1_row3 = rset->columnOptionalUint64("col1");
  const auto col2_row3 = rset->columnOptionalString("col2");

  ASSERT_EQ(col1_row3.value(), 11);
  ASSERT_EQ(col2_row3.value(), "The most popular Open Source database ag");

  // retrieve the forth row (null)
  ASSERT_TRUE(rset->next());

  const auto col1_row4 = rset->columnOptionalUint64("col1");
  const auto col2_row4 = rset->columnOptionalString("col2");

  ASSERT_EQ(col1_row4.value(), 1);
  ASSERT_TRUE(rset->columnIsNull("col2"));

  // retrieve the fifth row (null)
  ASSERT_TRUE(rset->next());

  const auto col1_row5 = rset->columnOptionalUint64("col1");
  const auto col2_row5 = rset->columnOptionalString("col2");

  ASSERT_TRUE(rset->columnIsNull("col1"));
  ASSERT_TRUE(rset->columnIsNull("col2"));


  // end
  ASSERT_FALSE(rset->next());

  std::list<std::string> tables = conn->getTableNames();
  std::string table_name = "test_table";
  ASSERT_TRUE(std::find(std::begin(tables), 
                        std::end(tables), 
                        table_name) != tables.end());

}


TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, drop_table_not_exists) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;


  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_drop_table = "DROP TABLE test_table_not_exist";
  stmt = conn->createStmt(sql_drop_table);
  ASSERT_THROW(stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON), exception::Exception);

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, delete_stmt) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;


  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_delete = "delete from test_table where col1=42";
  stmt = conn->createStmt(sql_delete);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, delete_and_insert) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;


  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_delete = "delete from ADMIN_USER where ADMIN_USER_NAME = :ADMIN_USER_NAME";
  stmt = conn->createStmt(sql_delete);
  stmt->bindString(":ADMIN_USER_NAME", "admin_user_name");
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);

  const std::string sql_insert = "INSERT INTO ADMIN_USER(ADMIN_USER_NAME,USER_COMMENT,CREATION_LOG_USER_NAME,CREATION_LOG_HOST_NAME,CREATION_LOG_TIME,LAST_UPDATE_USER_NAME,LAST_UPDATE_HOST_NAME,LAST_UPDATE_TIME)VALUES(:ADMIN_USER_NAME,:USER_COMMENT,:CREATION_LOG_USER_NAME,:CREATION_LOG_HOST_NAME,:CREATION_LOG_TIME,:LAST_UPDATE_USER_NAME,:LAST_UPDATE_HOST_NAME,:LAST_UPDATE_TIME)";
  stmt = conn->createStmt(sql_insert);

  const uint64_t now = time(nullptr);


  stmt->bindString(":ADMIN_USER_NAME", "admin_user_name");

  stmt->bindString(":USER_COMMENT", "comment");

  stmt->bindString(":CREATION_LOG_USER_NAME", "local_admin_user");
  stmt->bindString(":CREATION_LOG_HOST_NAME", "local_admin_host");
  stmt->bindUint64(":CREATION_LOG_TIME", now);

  stmt->bindString(":LAST_UPDATE_USER_NAME", "local_admin_user");
  stmt->bindString(":LAST_UPDATE_HOST_NAME", "local_admin_host");
  stmt->bindUint64(":LAST_UPDATE_TIME", now);

  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);


  const std::string sql_select = "SELECT ADMIN_USER_NAME AS ADMIN_USER_NAME FROM ADMIN_USER WHERE ADMIN_USER_NAME = :ADMIN_USER_NAME";
  stmt = conn->createStmt(sql_select);

  stmt->bindString(":ADMIN_USER_NAME", "admin_user_name");

  std::unique_ptr<Rset> rset = stmt->executeQuery(AutocommitMode::AUTOCOMMIT_OFF);

  while (rset->next()) {

    auto col1 = rset->columnOptionalString("ADMIN_USER_NAME");
    ASSERT_EQ(col1.value(), "admin_user_name");

  }

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, counter) {
  // https://dev.mysql.com/doc/refman/5.5/en/information-functions.html#function_last-insert-id
  /**
   * mysql> CREATE TABLE sequence (id INT NOT NULL);
   * mysql> INSERT INTO sequence VALUES (0);
   *
   * mysql> UPDATE sequence SET id=LAST_INSERT_ID(id+1);
   * mysql> SELECT LAST_INSERT_ID();
   *
   */

  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_drop_table = "DROP TABLE IF EXISTS sequence";
  stmt = conn->createStmt(sql_drop_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_table = "CREATE TABLE sequence (id INT NOT NULL)";
  stmt = conn->createStmt(sql_create_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_init_table = "INSERT INTO sequence VALUES (0)";
  stmt = conn->createStmt(sql_init_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();
  
  // Then, try to insert values
  int cnt = 100;
  for (int i = 0; i < cnt; ++i) {

    const std::string sql_update_table = "UPDATE sequence SET id=LAST_INSERT_ID(id+1)";
    stmt = conn->createStmt(sql_update_table);
    stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
    conn->commit();

    // now, get the id
    const std::string sql_query = "SELECT LAST_INSERT_ID() as id";

    // const std::string sql_query = "SELECT 123 as id";

    stmt = conn->createStmt(sql_query);
    std::unique_ptr<Rset> rset = stmt->executeQuery(AutocommitMode::AUTOCOMMIT_OFF);
    while(rset->next()) {
      const auto id = rset->columnOptionalUint64("id");
      ASSERT_EQ(id.value(), i+1);
      // std::cout << id.value() << std::endl;
    }
    // conn->commit();


  }
}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, get_null_uint64) {
  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_drop_table = "DROP TABLE IF EXISTS testtb_null_uint64";
  stmt = conn->createStmt(sql_drop_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_table = "CREATE TABLE testtb_null_uint64 (id INT)";
  stmt = conn->createStmt(sql_create_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_init_table = "INSERT INTO testtb_null_uint64 VALUES (NULL)";
  stmt = conn->createStmt(sql_init_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_init_table_0 = "INSERT INTO testtb_null_uint64 VALUES (0)";
  stmt = conn->createStmt(sql_init_table_0);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_init_table_1 = "INSERT INTO testtb_null_uint64 VALUES (1)";
  stmt = conn->createStmt(sql_init_table_1);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();


  const std::string sql_query = "SELECT id as id FROM testtb_null_uint64";

  // const std::string sql_query = "SELECT 123 as id";

  stmt = conn->createStmt(sql_query);
  std::unique_ptr<Rset> rset = stmt->executeQuery(AutocommitMode::AUTOCOMMIT_OFF);
  ASSERT_TRUE(rset->next());
  const auto id_null = rset->columnOptionalUint64("id");
  ASSERT_FALSE(id_null);
  
  ASSERT_TRUE(rset->next());
  const auto id_0 = rset->columnOptionalUint64("id");
  ASSERT_EQ(id_0.value(), 0);

  ASSERT_TRUE(rset->next());
  const auto id_1 = rset->columnOptionalUint64("id");
  ASSERT_EQ(id_1.value(), 1);

}


TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, simulate_check_constraint) {
  /**
   * http://www.mysqltutorial.org/mysql-check-constraint/
   */

  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_drop_table = "DROP TABLE IF EXISTS testtb_parts";
  stmt = conn->createStmt(sql_drop_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_drop_trigger = "DROP TRIGGER IF EXISTS parts_before_insert";
  dynamic_cast<MysqlConn*>(conn.get())->execute(sql_drop_trigger);
  
  const std::string sql_create_table = "CREATE TABLE testtb_parts ("\
    "part_no VARCHAR(18) PRIMARY KEY,"\
    "description VARCHAR(40),"\
    "cost DECIMAL(10 , 2 ) NOT NULL,"
    "price DECIMAL(10,2) NOT NULL"\
    ")";
  stmt = conn->createStmt(sql_create_table);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_trigger = ""
    "CREATE TRIGGER `parts_before_insert` BEFORE INSERT ON `testtb_parts` "
    "FOR EACH ROW "
    "BEGIN "
    "  IF new.cost < 0 THEN "
    "    SIGNAL SQLSTATE '45000' "
    "      SET MESSAGE_TEXT = 'check constraint on parts.cost failed'; "
    "  END IF; "
    "END"
    "";

  dynamic_cast<MysqlConn*>(conn.get())->execute(sql_create_trigger);

  const std::string sql_init_table_0 = "INSERT INTO testtb_parts VALUES ('A-000','Cooler',100,120)";
  stmt = conn->createStmt(sql_init_table_0);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_init_table_1 = "INSERT INTO testtb_parts VALUES ('A-001','Cooler',-100,120)";
  stmt = conn->createStmt(sql_init_table_1);
  ASSERT_THROW(stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON), exception::Exception);
  conn->commit();

  const std::string sql_init_table_2 = "INSERT INTO testtb_parts VALUES ('A-002','Cooler',200,220)";
  stmt = conn->createStmt(sql_init_table_2);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

}

TEST_F(DISABLED_cta_rdbms_wrapper_MysqlConnTest, create_trigger_failed_using_prepared_statement_with_fallback) {
  /**
   * http://www.mysqltutorial.org/mysql-check-constraint/
   */

  using namespace cta;
  using namespace cta::rdbms;
  using namespace cta::rdbms::wrapper;

  MysqlConnFactory conn_factory("localhost", "cta", "ctatest", "testdb", 3306);
  std::unique_ptr<Conn> conn;

  conn = conn_factory.create();
  ASSERT_TRUE(conn->isOpen());

  std::unique_ptr<Stmt> stmt;

  const std::string sql_drop_trigger = "DROP TRIGGER IF EXISTS parts_before_insert";
  stmt = conn->createStmt(sql_drop_trigger);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();

  const std::string sql_create_trigger = ""
    "CREATE TRIGGER `parts_before_insert` BEFORE INSERT ON `testtb_parts` "
    "FOR EACH ROW "
    "BEGIN "
    "  IF new.cost < 0 THEN "
    "    SIGNAL SQLSTATE '45000' "
    "      SET MESSAGE_TEXT = 'check constraint on parts.cost failed'; "
    "  END IF; "
    "END"
    "";
  stmt = conn->createStmt(sql_create_trigger);
  stmt->executeNonQuery(AutocommitMode::AUTOCOMMIT_ON);
  conn->commit();
}

} // namespace unitTests
