/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/server/RequestLogger.hpp"

#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>
#include <optional>
#include <string>

namespace unitTests {

using cta::grpc_server::RequestLogger;

class RequestLoggerTest : public ::testing::Test {
protected:
  RequestLoggerTest() : m_logger("dummy", "RequestLoggerTest", cta::log::DEBUG), m_lc(m_logger) {
    m_logger.setLogFormat("json");
  }

  bool logContains(const std::string& str) const { return m_logger.getLog().find(str) != std::string::npos; }

  cta::log::StringLogger m_logger;
  cta::log::LogContext m_lc;
};

TEST_F(RequestLoggerTest, logsStartAndSuccess) {
  RequestLogger requestLogger(m_lc, "test request");
  EXPECT_TRUE(logContains("Handling test request"));

  requestLogger.succeeded();
  EXPECT_TRUE(logContains("Test request succeeded"));
  EXPECT_TRUE(logContains("\"status\":\"success\""));
  EXPECT_TRUE(logContains("\"requestTime\":"));
  EXPECT_FALSE(logContains("\"reason\""));
}

TEST_F(RequestLoggerTest, logsUserErrorWithReason) {
  RequestLogger requestLogger(m_lc, "test request");
  requestLogger.rejected("bad option");

  EXPECT_TRUE(logContains("Test request rejected due to a user error"));
  EXPECT_TRUE(logContains("\"status\":\"user_error\""));
  EXPECT_TRUE(logContains("\"reason\":\"bad option\""));
  EXPECT_TRUE(logContains("\"log_level\":\"INFO\""));
}

TEST_F(RequestLoggerTest, logsFailureWithReason) {
  RequestLogger requestLogger(m_lc, "test request");
  requestLogger.failed("database is down");

  EXPECT_TRUE(logContains("Test request failed"));
  EXPECT_TRUE(logContains("\"status\":\"failure\""));
  EXPECT_TRUE(logContains("\"reason\":\"database is down\""));
  EXPECT_TRUE(logContains("\"log_level\":\"ERROR\""));
}

TEST_F(RequestLoggerTest, keepsParamsPresentAtConstruction) {
  std::optional<RequestLogger> requestLogger;
  {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("requestId", "abc");
    requestLogger.emplace(m_lc, "test request");
  }
  m_logger.clearLog();

  // The params were popped from the original context, but the logger kept its own copy of them
  requestLogger->succeeded();
  EXPECT_TRUE(logContains("\"requestId\":\"abc\""));
  EXPECT_EQ(0U, m_lc.size());
}

TEST_F(RequestLoggerTest, reportsCallerSourceLocation) {
  RequestLogger requestLogger(m_lc, "test request");
  m_logger.clearLog();

  const auto line = __LINE__ + 1;
  requestLogger.succeeded();
  EXPECT_TRUE(logContains("\"source_location\":\"RequestLoggerTest.cpp:" + std::to_string(line) + "\""));
}

}  // namespace unitTests
