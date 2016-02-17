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

class cta_log_SyslogLoggerTest: public ::testing::Test {
public:
  cta_log_SyslogLoggerTest()/*: m_log(cta::log::SOCKET_NAME, "unitttests",
    cta::log::DEBUG)*/ {
  }
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  //cta::log::TestingSyslogLogger m_log;
}; // class SyslogLoggerTest

//TEST_F(cta_log_SyslogLoggerTest, logMsgAndParamsList) {
//  using namespace cta::log;
//  std::list<Param> params;
//  params.push_back(Param("testParam", "value of test param"));
//
//  ASSERT_NO_THROW(
//    m_log(
//      LOG_INFO,
//      "cta_log_SyslogLoggerTest logMsgAndParamsList",
//      params));
//}
//
//TEST_F(cta_log_SyslogLoggerTest, logMsg) {
//  using namespace cta::log;
//
//  ASSERT_NO_THROW(
//    m_log(LOG_INFO, "Calling logger without parameters or time stamp"));
//}   

TEST_F(cta_log_SyslogLoggerTest, cleanStringWithoutReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = cta::log::Logger::cleanString(s, false);
  ASSERT_EQ(std::string("'Hello there World'"), cleaned);
}

TEST_F(cta_log_SyslogLoggerTest, cleanStringReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = cta::log::Logger::cleanString(s, true);
  ASSERT_EQ(std::string("'Hello_there_World'"), cleaned);
}

} // namespace unitTests
