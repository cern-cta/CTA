/******************************************************************************
 *                test/unittest/castor/tape/rmc/RmcDaemonTest.hpp
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

#include "castor/log/DummyLogger.hpp"
#include "test/unittest/castor/tape/rmc/TestingRmcDaemon.hpp"
#include "test/unittest/DummyOstream.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <sstream>
#include <string.h>

namespace castor {
namespace tape {
namespace rmc {

class RmcDaemonTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
    // Allow getopt_long to be called again
    optind = 0;
  }

  void testParceCommandLineWithNoArgs() {
    unittest::DummyOstream stdOut;
    unittest::DummyOstream stdErr;
    log::DummyLogger logger("unittest");
    TestingRmcDaemon daemon(stdOut, stdErr, logger);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground before parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help before parsing",
      false,
      daemon.m_cmdLine.help);

    const int argc = 1;
    char *argv[1] = {"rmcd"};
    daemon.parseCommandLine(argc, argv);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground after parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help after parsing",
      false,
      daemon.m_cmdLine.help);
  }

  void testParceCommandLineWithMinusH() {
    unittest::DummyOstream stdOut;
    unittest::DummyOstream stdErr;
    log::DummyLogger logger("unittest");
    TestingRmcDaemon daemon(stdOut, stdErr, logger);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground before parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help before parsing",
      false,
      daemon.m_cmdLine.help);

    const int argc = 2;
    char *argv[2] = {"rmcd", "-h"};
    daemon.parseCommandLine(argc, argv);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground after parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help after parsing",
      true,
      daemon.m_cmdLine.help);
  }

  void testParceCommandLineWithMinusMinusHelp() {
    unittest::DummyOstream stdOut;
    unittest::DummyOstream stdErr;
    log::DummyLogger logger("unittest");
    TestingRmcDaemon daemon(stdOut, stdErr, logger);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground before parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help before parsing",
      false,
      daemon.m_cmdLine.help);

    const int argc = 2;
    char *argv[2] = {"rmcd", "--help"};
    daemon.parseCommandLine(argc, argv);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground after parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help after parsing",
      true,
      daemon.m_cmdLine.help);
  }

  void testParceCommandLineWithMinusF() {
    unittest::DummyOstream stdOut;
    unittest::DummyOstream stdErr;
    log::DummyLogger logger("unittest");
    TestingRmcDaemon daemon(stdOut, stdErr, logger);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground before parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help before parsing",
      false,
      daemon.m_cmdLine.help);

    const int argc = 2;
    char *argv[2] = {"rmcd", "-f"};
    daemon.parseCommandLine(argc, argv);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground after parsing",
      true,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help after parsing",
      false,
      daemon.m_cmdLine.help);
  }

  void testParceCommandLineWithMinusMinusForeground() {
    unittest::DummyOstream stdOut;
    unittest::DummyOstream stdErr;
    log::DummyLogger logger("unittest");
    TestingRmcDaemon daemon(stdOut, stdErr, logger);
  
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground before parsing",
      false,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help before parsing",
      false,
      daemon.m_cmdLine.help);

    const int argc = 2;
    char *argv[2] = {"rmcd", "--foreground"};
    daemon.parseCommandLine(argc, argv);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.foreground after parsing",
      true,
      daemon.m_cmdLine.foreground);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing m_cmdLine.help after parsing",
      false,
      daemon.m_cmdLine.help);
  }

  CPPUNIT_TEST_SUITE(RmcDaemonTest);
  CPPUNIT_TEST(testParceCommandLineWithNoArgs);
  CPPUNIT_TEST(testParceCommandLineWithMinusH);
  CPPUNIT_TEST(testParceCommandLineWithMinusMinusHelp);
  CPPUNIT_TEST(testParceCommandLineWithMinusF);
  CPPUNIT_TEST(testParceCommandLineWithMinusMinusForeground);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(RmcDaemonTest);

} // namespace rmc
} // namespace tape
} // namespace castor
