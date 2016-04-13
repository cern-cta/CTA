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
#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciEnv.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_catalogue_OcciEnvTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_OcciEnvTest, constructor) {
  using namespace cta::catalogue;
  OcciEnv env;
}

TEST_F(cta_catalogue_OcciEnvTest, createConn_null_username) {
  using namespace cta::catalogue;
  OcciEnv env;

  const char *const username = NULL;
  const char *const password = "Not NULL";
  const char *const database = "Not NULL";

  ASSERT_THROW(env.createConn(username, password, database), std::exception);
}

TEST_F(cta_catalogue_OcciEnvTest, createConn_null_password) {
  using namespace cta::catalogue;
  OcciEnv env;

  const char *const username = "Not NULL";
  const char *const password = NULL;
  const char *const database = "Not NULL";

  ASSERT_THROW(env.createConn(username, password, database), std::exception);
}

TEST_F(cta_catalogue_OcciEnvTest, createConn_null_database) {
  using namespace cta::catalogue;
  OcciEnv env;

  const char *const username = "Not NULL";
  const char *const password = "Not NULL";
  const char *const database = NULL;

  ASSERT_THROW(env.createConn(username, password, database), std::exception);
}

} // namespace unitTests
