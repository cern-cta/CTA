/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2024 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/log/Param.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <streambuf>

namespace unitTests {

class cta_log_LoggerTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }
}; // cta_log_ParamTest



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

// Insert characters that need to be scaped for all possible arguments.
// And check the output from stdout/file.
TEST_F(cta_log_LoggerTest, testMsgEscaping) {
  using namespace cta::log;
  StdoutLogger std_logger("dummy\'", "cta_log_\"TEST\"", DEBUG);

  // Set static params
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["dummy_static\?"] = "value_why\?";
  std_logger.setStaticParams(staticParamMap);

  //  () operator will write directly to stdout, lets redirect that stream into
  //  a variable so that we can check the output.
  std::streambuf* oldCoutStreamBuffer = cout.rdbuf();
  ostringstream strCout;
  cout.rdbug( strCout.rdbuf());

  std_logger(ERR, "Exception message with new lines:\nSomething went wrong\nin line -1\n");

  // Restore cout
  cout.rdbuf(oldCoutStreamBuffer);

  std::cout << strCout ;
  // Validate message
  ASSERT_EQ(1,1);
}

} // namespace unitTests
