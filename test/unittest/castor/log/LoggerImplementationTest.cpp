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

#include <cppunit/extensions/HelperMacros.h>
#include <memory>
#include <sys/time.h>

namespace castor {
namespace log {

class SyslogLoggerTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testLogMsgWithAllParams() {
    std::auto_ptr<Logger> logger;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Constructing logger",
      logger.reset(new SyslogLogger("unitttests")));

    const int numParams = 1;
    const Param params[1] = {Param("testParam", "valueOfTestParam")};
    struct timeval timeStamp;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "gettimeofday()",
      0,
      gettimeofday(&timeStamp, NULL));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Calling logMsg() with all parameters",
      logger->logMsg(
        LOG_INFO,
        "Calling logMsg() with all parameters",
        numParams,
        params,
        timeStamp));
  }

  void testLogMsgWithoutTimeStamp() {
    std::auto_ptr<Logger> logger;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Constructing logger",
      logger.reset(new SyslogLogger("unitttests")));

    const int numParams = 1;
    const Param params[1] = {Param("testParam", "valueOfTestParam")};

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Calling logMsg() without time stamp",
      logger->logMsg(
        LOG_INFO,
        "Calling logMsg() without time stamp",
        numParams,
        params));
  }

  void testLogMsgWithoutParamsOrTimeStamp() {
    std::auto_ptr<Logger> logger;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Constructing logger",
      logger.reset(new SyslogLogger("unitttests")));
  
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Calling logMsg() without parameters or time stamp",
      logger->logMsg(
        LOG_INFO,
        "Calling logMsg() without parameters or time stamp"));
  }   

  CPPUNIT_TEST_SUITE(SyslogLoggerTest);
  CPPUNIT_TEST(testLogMsgWithAllParams);
  CPPUNIT_TEST(testLogMsgWithoutTimeStamp);
  CPPUNIT_TEST(testLogMsgWithoutParamsOrTimeStamp);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(SyslogLoggerTest);

} // namespace log
} // namespace castor
