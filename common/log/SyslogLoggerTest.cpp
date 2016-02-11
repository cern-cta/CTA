/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/log/SyslogLogger.hpp"
#include "common/log/TestingSyslogLogger.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/time.h>

namespace unitTests {

class cta_log__SyslogLoggerTest: public ::testing::Test {
public:
  cta_log__SyslogLoggerTest(): m_log("unitttests") {
  }
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  cta::log::TestingSyslogLogger m_log;
}; // class SyslogLoggerTest

TEST_F(cta_log__SyslogLoggerTest, logMsgParamsVectorAndTimeStamp) {
  using namespace cta::log;
  std::vector<Param> params;
  params.push_back(Param("testParam", "value of test param"));
  struct timeval timeStamp;

  ASSERT_EQ(0, gettimeofday(&timeStamp, NULL));

  ASSERT_NO_THROW(m_log(
    LOG_INFO,
    "cta_log__SyslogLoggerTest logMsgParamsVectorAndTimeStamp",
    params,
    timeStamp));
}

TEST_F(cta_log__SyslogLoggerTest, logMsgParamsListAndTimeStamp) {
  using namespace cta::log;
  std::list<Param> params;
  params.push_back(Param("testParam", "value of test param"));
  struct timeval timeStamp;

  ASSERT_EQ(0, gettimeofday(&timeStamp, NULL));

  ASSERT_NO_THROW(m_log(
    LOG_INFO,
    "cta_log__SyslogLoggerTest logMsgParamsListAndTimeStamp",
    params,
    timeStamp));
}

TEST_F(cta_log__SyslogLoggerTest, logMsgParamsArrayAndTimeStamp) {
  using namespace cta::log;
  const int numParams = 1;
  const Param params[1] = {Param("testParam", "value of test param")};
  struct timeval timeStamp;

  ASSERT_EQ(0, gettimeofday(&timeStamp, NULL));

  ASSERT_NO_THROW(m_log(
    LOG_INFO,
    "cta_log__SyslogLoggerTest logMsgParamsArrayAndTimeStamp",
    numParams,
    params,
    timeStamp));
}

TEST_F(cta_log__SyslogLoggerTest, logMsgAndParamsVector) {
  using namespace cta::log;
  std::vector<Param> params;
  params.push_back(Param("testParam", "value of test param"));

  ASSERT_NO_THROW(m_log(
      LOG_INFO,
      "cta_log__SyslogLoggerTest logMsgAndParamsVector",
      params));
}

TEST_F(cta_log__SyslogLoggerTest, logMsgAndParamsList) {
  using namespace cta::log;
  std::list<Param> params;
  params.push_back(Param("testParam", "value of test param"));

  ASSERT_NO_THROW(
    m_log(
      LOG_INFO,
      "cta_log__SyslogLoggerTest logMsgAndParamsList",
      params));
}

TEST_F(cta_log__SyslogLoggerTest, logMsgAndParamsArray) {
  using namespace cta::log;
  const int numParams = 1;
  const Param params[1] = {Param("testParam", "value of test param")};

  ASSERT_NO_THROW(
    m_log(
      LOG_INFO,
      "cta_log__SyslogLoggerTest logMsgAndParamsArray",
      numParams,
      params));
}

TEST_F(cta_log__SyslogLoggerTest, logMsg) {
  using namespace cta::log;

  ASSERT_NO_THROW(
    m_log(LOG_INFO, "Calling logger without parameters or time stamp"));
}   

TEST_F(cta_log__SyslogLoggerTest, cleanStringWithoutReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = m_log.cleanString(s, false);
  ASSERT_EQ(std::string("'Hello there World'"), cleaned);
}

TEST_F(cta_log__SyslogLoggerTest, cleanStringReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = m_log.cleanString(s, true);
  ASSERT_EQ(std::string("'Hello_there_World'"), cleaned);
}

} // namespace unitTests
