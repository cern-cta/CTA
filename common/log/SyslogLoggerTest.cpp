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

#include "common/log/SyslogLogger.hpp"
#include "common/log/TestingSyslogLogger.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/time.h>

using namespace cta::log;

namespace unitTests {
  
class cta_log_SyslogLoggerTest: public ::testing::Test {
public:
  cta_log_SyslogLoggerTest(): m_log("dummy", "unitttests") {
  }
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
  TestingSyslogLogger m_log;
  
}; // class SyslogLoggerTest

TEST_F(cta_log_SyslogLoggerTest, logMsgAndParamsList) {
  std::list<Param> params;
  params.push_back(Param("testParam", "value of test param"));

  ASSERT_NO_THROW(
    m_log(
      INFO,
      "cta_log_SyslogLoggerTest logMsgAndParamsList",
      params));
}

TEST_F(cta_log_SyslogLoggerTest, logMsg) {
  ASSERT_NO_THROW(
    m_log(INFO, "Calling logger without parameters"));
}   

TEST_F(cta_log_SyslogLoggerTest, cleanStringWithoutReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = Logger::cleanString(s, false);
  ASSERT_EQ(std::string("'Hello there World'"), cleaned);
}

TEST_F(cta_log_SyslogLoggerTest, cleanStringReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = Logger::cleanString(s, true);
  ASSERT_EQ(std::string("'Hello_there_World'"), cleaned);
}

} // namespace unitTests
