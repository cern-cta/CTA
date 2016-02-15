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

#include "castor/log/SyslogLogger.hpp"
#include "castor/log/TestingSyslogLogger.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class castor_log_SyslogLoggerTest: public ::testing::Test {
public:
  castor_log_SyslogLoggerTest(): m_log("unitttests") {
  }
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  castor::log::TestingSyslogLogger m_log;
}; // class SyslogLoggerTest

TEST_F(castor_log_SyslogLoggerTest, logMsgAndParamsList) {
  using namespace castor::log;
  std::list<Param> params;
  params.push_back(Param("testParam", "value of test param"));

  ASSERT_NO_THROW(
    m_log(
      LOG_INFO,
      "castor_log_SyslogLoggerTest logMsgAndParamsList",
      params));
}

TEST_F(castor_log_SyslogLoggerTest, logMsg) {
  using namespace castor::log;

  ASSERT_NO_THROW(
    m_log(LOG_INFO, "Calling logger without parameters"));
}   

TEST_F(castor_log_SyslogLoggerTest, cleanStringWithoutReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = m_log.cleanString(s, false);
  ASSERT_EQ(std::string("'Hello there World'"), cleaned);
}

TEST_F(castor_log_SyslogLoggerTest, cleanStringReplacingUnderscores) {
  const std::string s("  \t\t\n\n\"Hello there\tWorld\"  \t\t\n\n");
  const std::string cleaned = m_log.cleanString(s, true);
  ASSERT_EQ(std::string("'Hello_there_World'"), cleaned);
}

} // namespace unitTests
