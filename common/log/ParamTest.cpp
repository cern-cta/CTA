/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/Param.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_log_LoggerTest : public ::testing::Test {
protected:
  void SetUp() {}

  void TearDown() {}
};  // cta_log_ParamTest

// Tests for floating point formatting.
TEST_F(cta_log_LoggerTest, testConstructorWithAString) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", "Value")));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("Value"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithAStringNoName) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("", "Value")));
  ASSERT_EQ(std::string(""), param->getName());
  ASSERT_EQ(std::string("Value"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithAnInt) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", -1234)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("-1234"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithAnUInt) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234u)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithALargeUInt) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  // Confirm assumption that max uint64_t cannot fit in a int64_t
  ASSERT_EQ("-1", std::to_string(static_cast<int64_t>(std::numeric_limits<uint64_t>::max())));

  ASSERT_NO_THROW(param.reset(new Param("Name", std::numeric_limits<uint64_t>::max())));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::to_string(std::numeric_limits<uint64_t>::max()), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithAFloat) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.12f)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.12"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithADouble1) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.123456789)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.123456789"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithADouble2) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.0000)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.0"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithADouble3) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 0.0000)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("0.0"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithADouble4) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1.234e-20)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1.234e-20"), param->getValueStr());
}

TEST_F(cta_log_LoggerTest, testConstructorWithADouble5) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1e+30)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1e+30"), param->getValueStr());
}
}  // namespace unitTests
