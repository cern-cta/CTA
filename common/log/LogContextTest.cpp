/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>

#include <regex>

using namespace cta::log;

namespace unitTests {
  TEST(cta_log_LogContextTest, additionScopedRemove) {
    DummyLogger dl("dummy", "cta_log_LogContextTest");
    LogContext lc(dl);
    lc.pushOrReplace(Param("MigrationRequestId", 123));
    ASSERT_EQ(1U, lc.size());
    {
      // Create an anonymous variable (for its scope)
      LogContext::ScopedParam sp(lc, Param("fileId", 12345));
      ASSERT_EQ(2U, lc.size());
      lc.log(DEBUG, "Two params message");
      {
        // Test that we do not allow duplicate params
        LogContext::ScopedParam sp(lc, Param("fileId", 123456));
        ASSERT_EQ(2U, lc.size());
        LogContext::ScopedParam sp2(lc, Param("TPVID", "T1234"));
        ASSERT_EQ(3U, lc.size());
      }
    }
    ASSERT_EQ(1U, lc.size());
    lc.log(DEBUG, "One param message");
    lc.erase("MigrationRequestId");
    ASSERT_EQ(0U, lc.size());
  }
  
  TEST(cta_log_LogContextTest, paramsFound) {
    StringLogger sl ("dummy", "cta_log_LogContextTest", DEBUG);
    LogContext lc(sl);
    lc.pushOrReplace(Param("MigrationRequestId", 123));
    lc.log(INFO, "First log");
    std::string first = sl.getLog();
    ASSERT_NE(std::string::npos, first.find("MigrationRequestId"));
    {
      LogContext::ScopedParam sp(lc, Param("fileId", 12345));
      lc.log(INFO, "Second log");
    }
    std::string second = sl.getLog();
    ASSERT_NE(std::string::npos, second.find("fileId"));
    // We expect the NSFILEID parameter to show up only once (i.e, not after 
    // offset, which marks the end of its first occurrence).
    lc.log(INFO, "Third log");
    std::string third = sl.getLog();
    size_t offset  = third.find("fileId") + strlen("fileId");
    ASSERT_EQ(std::string::npos, third.find("fileId", offset));
  }

  TEST(cta_log_LogContextTest, logMessageEscaping) {
    StringLogger sl("dummy\"", "cta_log_LogContextTest_escaped\"", DEBUG);
    sl.setLogFormat("json");

    // Set static params
    std::map<std::string, std::string> staticParamMap;
    staticParamMap["dummy_static\""] = "value_why\"";
    sl.setStaticParams(staticParamMap);

    // Set a param with a character to be escaped.
    LogContext lc(sl);
    lc.pushOrReplace(Param("valid_\"key", "Valid \n out"));
    lc.log(INFO, "Split message\n by newline");
    std::cout << sl.getLog();

    std::regex regex_pattern(R"(^\{"epoch_time":\d+\.\d+,"local_time":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}","hostname":"dummy\\\"","program":"cta_log_LogContextTest_escaped\\\"","log_level":"INFO","pid":\d+,"tid":\d+,"message":"Split message\\n by newline","dummy_static\\\"":"value_why\\\"","valid_\\\"key":"Valid \\n out"\}\n$)");
    EXPECT_TRUE(std::regex_match(sl.getLog(), regex_pattern));
  }
}
