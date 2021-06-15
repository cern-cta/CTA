/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "common/log/LogLevel.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_log_LogLevelTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class cta_log_LogLevelTest

TEST_F(cta_log_LogLevelTest, toLogLevel_EMERG) {
  using namespace cta::log;

  ASSERT_EQ(EMERG, toLogLevel("EMERG"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_ALERT) {
  using namespace cta::log;

  ASSERT_EQ(ALERT, toLogLevel("ALERT"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_CRIT) {
  using namespace cta::log;

  ASSERT_EQ(CRIT, toLogLevel("CRIT"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_ERR) {
  using namespace cta::log;

  ASSERT_EQ(ERR, toLogLevel("ERR"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_WARNING) {
  using namespace cta::log;

  ASSERT_EQ(WARNING, toLogLevel("WARNING"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_NOTICE) {
  using namespace cta::log;

  ASSERT_EQ(NOTICE, toLogLevel("NOTICE"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_INFO) {
  using namespace cta::log;

  ASSERT_EQ(INFO, toLogLevel("INFO"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_DEBUG) {
  using namespace cta::log;

  ASSERT_EQ(DEBUG, toLogLevel("DEBUG"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_USERERR) {
  using namespace cta::log;

  // It is a convention of CTA to use syslog level of LOG_NOTICE to label
  // user errors.
  ASSERT_EQ(NOTICE, toLogLevel("USERERR"));
}

TEST_F(cta_log_LogLevelTest, toLogLevel_INVALID_LOG_LEVEL) {
  using namespace cta;
  using namespace cta::log;

  ASSERT_THROW(toLogLevel("toLogLevel_INVALID_LOG_LEVEL"), exception::Exception);
}

} // namespace unitTests
