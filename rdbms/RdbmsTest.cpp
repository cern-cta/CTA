/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/rdbms.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_rdbmsTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_rdbms_rdbmsTest, getSqlForException) {
  using namespace cta::rdbms;

  const std::string::size_type maxSqlLenInExceptions = 7;
  const std::string orginalSql = "1234567890";
  const std::string expectedSql = "1234...";

  const std::string resultingSql = getSqlForException(orginalSql, maxSqlLenInExceptions);
  ASSERT_EQ(expectedSql, resultingSql);
}

}  // namespace unitTests
