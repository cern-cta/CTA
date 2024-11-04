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
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <streambuf>
#include <regex>

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
  StringLogger str_logger("dummy\"", "cta_log_\"TEST\"", DEBUG);
  str_logger.setLogFormat("json");

  // Set static params
  std::map<std::string, std::string> staticParamMap;
  staticParamMap["dummy_static\""] = "value_why\"";
  str_logger.setStaticParams(staticParamMap);

  str_logger(ERR, "Exception message with new lines:\nSomething went wrong\nin line -1\n");

  // Validate message
  std::regex regex_pattern(
    R"(^\{"epoch_time":\d+\.\d+,"local_time":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}","hostname":"dummy\\"","program":"cta_log_\\"TEST\\"","log_level":"ERROR","pid":\d+,"tid":\d+,"message":"Exception message with new lines:\\n.*?\\n.*?\\n.*?","dummy_static\\"":"value_why\\""\}\n$)");

  EXPECT_TRUE(std::regex_match(str_logger.getLog(), regex_pattern));
}

}  // namespace unitTests
