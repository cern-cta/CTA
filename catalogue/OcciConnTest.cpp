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

#include "catalogue/OcciEnv.hpp"
#include "catalogue/OcciConn.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_OcciConnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_OcciConnTest, constructor_null_connection) {
  using namespace cta;
  using namespace cta::catalogue;

  OcciEnv env;
  oracle::occi::Connection *const underlyingOcciConn = NULL;
  std::unique_ptr<OcciConn> conn;
  ASSERT_THROW(conn.reset(new OcciConn(env, underlyingOcciConn)),
    exception::Exception);
}

} // namespace unitTests
