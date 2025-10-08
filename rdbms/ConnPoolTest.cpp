/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_ConnPoolTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_ConnPoolTest, getPooledConn) {
  using namespace cta::rdbms;

  const Login login = Login::getInMemory();
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();
}

TEST_F(cta_rdbms_ConnPoolTest, getPooledConn_maxNbConns_zero) {
  using namespace cta::rdbms;

  const Login login = Login::getInMemory();
  const uint64_t maxNbConns = 0;
  ConnPool pool(login, maxNbConns);

  ASSERT_THROW(Conn conn = pool.getConn(), ConnPool::ConnPoolConfiguredWithZeroConns);
}

TEST_F(cta_rdbms_ConnPoolTest, assignment) {
  using namespace cta::rdbms;

  const Login login = Login::getInMemory();
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();

  Conn conn2(nullptr, nullptr);

  conn2 = pool.getConn();
}

TEST_F(cta_rdbms_ConnPoolTest, moveConstructor) {
  using namespace cta::rdbms;

  const Login login = Login::getInMemory();
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();

  Conn conn2(std::move(conn));
}

} // namespace unitTests
