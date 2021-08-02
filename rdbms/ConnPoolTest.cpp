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

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();
}

TEST_F(cta_rdbms_ConnPoolTest, getPooledConn_maxNbConns_zero) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  const uint64_t maxNbConns = 0;
  ConnPool pool(login, maxNbConns);

  ASSERT_THROW(Conn conn = pool.getConn(), ConnPool::ConnPoolConfiguredWithZeroConns);
}

TEST_F(cta_rdbms_ConnPoolTest, assignment) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();

  Conn conn2(nullptr, nullptr);

  conn2 = pool.getConn();
}

TEST_F(cta_rdbms_ConnPoolTest, moveConstructor) {
  using namespace cta::rdbms;

  const Login login(Login::DBTYPE_SQLITE, "", "", "file::memory:?cache=shared", "", 0);
  const uint64_t maxNbConns = 2;
  ConnPool pool(login, maxNbConns);

  Conn conn = pool.getConn();

  Conn conn2(std::move(conn));
}

} // namespace unitTests
