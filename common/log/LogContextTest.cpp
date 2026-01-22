/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/LogContext.hpp"

#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>
#include <regex>

using namespace cta::log;

namespace unitTests {
TEST(cta_log_LogContextTest, additionScopedRemove) {
  DummyLogger dl("dummy", "cta_log_LogContextTest");
  LogContext lc(dl);
  lc.push(Param("MigrationRequestId", 123));
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
  lc.pop("MigrationRequestId");
  ASSERT_EQ(0U, lc.size());
}

TEST(cta_log_LogContextTest, paramPushPopErase) {
  StringLogger sl("dummy", "cta_log_LogContextTest", DEBUG);
  LogContext lc(sl);
  // Push 1st
  lc.push(Param("MigrationRequestId", 111));
  {
    lc.log(INFO, "First log");
    std::string str = sl.getLog();
    ASSERT_NE(std::string::npos, str.find(R"(MigrationRequestId="111")"));
    sl.clearLog();
  }
  // Push 2nd
  lc.push(Param("MigrationRequestId", 222));
  {
    lc.log(INFO, "Second log");
    std::string str = sl.getLog();
    ASSERT_NE(std::string::npos, str.find(R"(MigrationRequestId="222")"));
    sl.clearLog();
  }
  // Push 3rd
  lc.push(Param("MigrationRequestId", 333));
  {
    lc.log(INFO, "Third log");
    std::string str = sl.getLog();
    ASSERT_NE(std::string::npos, str.find(R"(MigrationRequestId="333")"));
    sl.clearLog();
  }
  // Pop 3rd
  lc.pop("MigrationRequestId");
  {
    lc.log(INFO, "Second log again");
    std::string str = sl.getLog();
    ASSERT_NE(std::string::npos, str.find(R"(MigrationRequestId="222")"));
    sl.clearLog();
  }
  // Erase all
  lc.erase({"MigrationRequestId"});
  {
    lc.log(INFO, "Erase all");
    std::string str = sl.getLog();
    ASSERT_EQ(std::string::npos, str.find(R"(MigrationRequestId)"));
    sl.clearLog();
  }
}

TEST(cta_log_LogContextTest, paramScopes) {
  StringLogger sl("dummy", "cta_log_LogContextTest", DEBUG);
  LogContext lc(sl);
  {
    // 1st scope
    ScopedParamContainer sp1(lc);
    sp1.add("MigrationRequestId", 111);

    lc.log(INFO, "Test");
    std::string str1a = sl.getLog();
    sl.clearLog();
    ASSERT_NE(std::string::npos, str1a.find(R"(MigrationRequestId="111")"));

    {
      // 2nd scope
      ScopedParamContainer sp2(lc);
      sp2.add("MigrationRequestId", 222);

      lc.log(INFO, "Test");
      std::string str2a = sl.getLog();
      sl.clearLog();
      ASSERT_NE(std::string::npos, str2a.find(R"(MigrationRequestId="222")"));

      {
        // 3rd scope
        ScopedParamContainer sp3(lc);
        sp3.add("MigrationRequestId", 333);

        lc.log(INFO, "Test");
        std::string str3a = sl.getLog();
        sl.clearLog();
        ASSERT_NE(std::string::npos, str3a.find(R"(MigrationRequestId="333")"));
      }

      // 2nd scope again
      lc.log(INFO, "Test");
      std::string str2b = sl.getLog();
      sl.clearLog();
      ASSERT_NE(std::string::npos, str2b.find(R"(MigrationRequestId="222")"));
    }
    // 1st scope again
    lc.log(INFO, "Test");
    std::string str1b = sl.getLog();
    sl.clearLog();
    ASSERT_NE(std::string::npos, str1b.find(R"(MigrationRequestId="111")"));
  }
  // Out of scope
  lc.log(INFO, "Test");
  std::string str0b = sl.getLog();
  ASSERT_EQ(std::string::npos, str0b.find(R"(MigrationRequestId)"));
  sl.clearLog();
}

TEST(cta_log_LogContextTest, paramsFound) {
  StringLogger sl("dummy", "cta_log_LogContextTest", DEBUG);
  LogContext lc(sl);
  lc.push(Param("MigrationRequestId", 123));
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
  size_t offset = third.find("fileId") + strlen("fileId");
  ASSERT_EQ(std::string::npos, third.find("fileId", offset));
}

TEST(cta_log_LogContextTest, logMessageEscaping) {
  StringLogger sl(R"(dummy")", R"(cta_log_LogContextTest_escaped")", DEBUG);
  sl.setLogFormat("json");

  // Set static params
  std::map<std::string, std::string> staticParamMap;
  staticParamMap[R"(dummy_static")"] = R"(value_why")";
  sl.setStaticParams(staticParamMap);

  // Set a param with a character to be escaped.
  LogContext lc(sl);
  lc.push(Param(R"(valid_"key)", "Valid \n out"));
  lc.log(INFO, "Split message\n by newline");

  std::regex regex_pattern(
    R"(^\{"epoch_time":\d+\.\d+,"local_time":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}","hostname":"dummy\\"","program":"cta_log_LogContextTest_escaped\\"","log_level":"INFO","pid":\d+,"tid":\d+,"message":"Split message\\n by newline","dummy_static\\"":"value_why\\"","valid_\\"key":"Valid \\n out"\}\n$)");
  EXPECT_TRUE(std::regex_match(sl.getLog(), regex_pattern));
}
}  // namespace unitTests
