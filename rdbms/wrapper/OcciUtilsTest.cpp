/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciUtils.cpp"

#include <gtest/gtest.h>

namespace unitTests {

using cta::rdbms::wrapper::OcciUtils;

class cta_rdbms_wrapper_OcciUtilsTest : public ::testing::Test {
protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(cta_rdbms_wrapper_OcciUtilsTest, test_connection_error) {
  std::runtime_error connection_error("ORA-25401: can not continue fetches");
  ASSERT_TRUE(OcciUtils::isLostConnection(connection_error));
}

TEST_F(cta_rdbms_wrapper_OcciUtilsTest, test_non_connection_error) {
  std::runtime_error connection_error("ORA-00001: unique constraint violated");
  ASSERT_FALSE(OcciUtils::isLostConnection(connection_error));
}

}  // namespace unitTests
