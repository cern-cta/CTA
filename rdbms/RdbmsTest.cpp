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

#include "rdbms/rdbms.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_rdbmsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_rdbmsTest, getSqlForException) {
  using namespace cta::rdbms;

  const std::string::size_type maxSqlLenInExceptions = 7;
  const std::string orginalSql = "1234567890";
  const std::string expectedSql = "1234...";

  const std::string resultingSql = getSqlForException(orginalSql, maxSqlLenInExceptions);
  ASSERT_EQ(expectedSql, resultingSql);
}

} // namespace unitTests
