/******************************************************************************
 *                castor/log/LoggerImplementationTest.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/log/LoggerImplementation.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/time.h>

namespace unitTests {

class castor_log_LoggerImplementationTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class LoggerImplementationTest

TEST_F(castor_log_LoggerImplementationTest, testLogMsgWithAllParams) {
  using namespace castor::log;
  LoggerImplementation log("unitttests");
  const int numParams = 1;
  const Param params[1] = {Param("testParam", "valueOfTestParam")};
  struct timeval timeStamp;

  ASSERT_EQ(0, gettimeofday(&timeStamp, NULL));

  ASSERT_NO_THROW(
    log(
      LOG_INFO,
      "Calling logger with all parameters",
      numParams,
      params,
      timeStamp));
}

TEST_F(castor_log_LoggerImplementationTest, testLogMsgWithoutTimeStamp) {
  using namespace castor::log;
  LoggerImplementation log("unitttests");
  const int numParams = 1;
  const Param params[1] = {Param("testParam", "valueOfTestParam")};

  ASSERT_NO_THROW(
    log(
      LOG_INFO,
      "Calling logger without time stamp",
      numParams,
      params));
}

TEST_F(castor_log_LoggerImplementationTest, testLogMsgWithoutParamsOrTimeStamp) {
  using namespace castor::log;
  LoggerImplementation log("unitttests");

  ASSERT_NO_THROW(
    log(LOG_INFO, "Calling logger without parameters or time stamp"));
}   

} // namespace unitTests
