/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/Param.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <regex>
#include <streambuf>

namespace unitTests {

class cta_log_LoggerTest : public ::testing::Test {
protected:
  void SetUp() {}

  void TearDown() {}
};  // cta_log_ParamTest

// Insert characters that need to be scaped for all possible arguments.
// And check the output from stdout/file.
TEST_F(cta_log_LoggerTest, testLogMsgEscaping) {
  using namespace cta::log;
  StringLogger str_logger(R"(dummy")", R"(cta_log_"TEST")", DEBUG);
  str_logger.setLogFormat("json");

  // Set static params
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["dummy_static\""] = R"(value_static")";
  str_logger.setStaticParams(staticParamMap);

  str_logger(ERR, "Exception message with new lines:\nSomething went wrong\nin line -1\n");
  // Validate message
  std::regex regex_pattern(
    R"(^\{"epoch_time":\d+\.\d+,"local_time":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}","hostname":"dummy\\"","program":"cta_log_\\"TEST\\"","log_level":"ERROR","pid":\d+,"tid":\d+,"message":"Exception message with new lines:\\nSomething went wrong\\nin line -1\\n","dummy_static\\"":"value_static\\""\}\n$)");

  EXPECT_TRUE(std::regex_match(str_logger.getLog(), regex_pattern));
}

}  // namespace unitTests
