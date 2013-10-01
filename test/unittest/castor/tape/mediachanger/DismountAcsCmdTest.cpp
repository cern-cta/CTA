/******************************************************************************
 *             test/unittest/castor/tape/mediachanger/DismountAcsCmdTest.hpp
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

#include "test/unittest/castor/tape/mediachanger/MockAcs.hpp"
#include "test/unittest/castor/tape/mediachanger/TestingDismountAcsCmd.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <list>
#include <memory>
#include <sstream>
#include <string.h>

namespace castor {
namespace tape {
namespace mediachanger {

class DismountAcsCmdTest: public CppUnit::TestFixture {
private:

  struct Argcv {
    int argc;
    char **argv;
    Argcv(): argc(0), argv(NULL) {
    }
  };
  typedef std::list<Argcv*> ArgcvList;
  ArgcvList m_argsList;

  /**
   * Creates a duplicate string usin the new operator.
   */
  char *dupString(const char *str) {
    const size_t len = strlen(str);
    char *duplicate = new char[len+1];
    strncpy(duplicate, str, len);
    duplicate[len] = '\0';
    return duplicate;
  }

public:

  void setUp() {
  }

  void tearDown() {
    // Allow getopt_long to be called again
    optind = 0;

    for(ArgcvList::const_iterator itor = m_argsList.begin();
      itor != m_argsList.end(); itor++) {
      for(int i=0; i < (*itor)->argc; i++) {
        delete[] (*itor)->argv[i];
      }
      delete[] (*itor)->argv;
      delete *itor;
    }
  }

  void testParceCmdLineWithNoArgs() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 1;
    args->argv = new char *[2];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling parseCmdLine with no arguments",
      cmd.parseCmdLine(args->argc, args->argv),
      castor::exception::MissingOperand);
  }

  void testParceCmdLineWithOnlyVolId() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 2;
    args->argv = new char *[3];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("VIDVID");
    args->argv[2] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Calling parseCmdLine with only volume identfier",
      cmd.parseCmdLine(args->argc, args->argv),
        castor::exception::MissingOperand);
  }

  void testParceCmdLineWithShortForce() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 4;
    args->argv = new char *[5];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("-f");
    args->argv[2] = dupString("VIDVID");
    args->argv[3] = dupString("111:112:113:114");
    args->argv[4] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing force is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.force);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithLongForce() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 4;
    args->argv = new char *[5];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--force");
    args->argv[2] = dupString("VIDVID");
    args->argv[3] = dupString("111:112:113:114");
    args->argv[4] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing force is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.force);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithShortHelp() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 2;
    args->argv = new char *[3];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("-h");
    args->argv[2] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing help is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.help);
  }

 void testParceCmdLineWithLongHelp() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 2;
    args->argv = new char *[3];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--help");
    args->argv[2] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing help is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.help);
  }

  void testParceCmdLineWithVolIdAndDrive() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 3;
    args->argv = new char *[4];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("VIDVID");
    args->argv[2] = dupString("111:112:113:114");
    args->argv[3] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Testing valid volume identfier and drive",
      cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing debug is not set",
      (BOOLEAN)FALSE, cmdLine.debug);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing help is not set",
      (BOOLEAN)FALSE, cmdLine.help);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing query is set to the default",
      10, cmdLine.queryInterval);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing timeout is set to the default",
      600, cmdLine.timeout);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithShortDebug() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 4;
    args->argv = new char *[5];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("-d");
    args->argv[2] = dupString("VIDVID");
    args->argv[3] = dupString("111:112:113:114");
    args->argv[4] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing debug is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.debug);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithLongDebug() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 4;
    args->argv = new char *[5];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--debug");
    args->argv[2] = dupString("VIDVID");
    args->argv[3] = dupString("111:112:113:114");
    args->argv[4] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing debug is set after command line is parsed",
      (BOOLEAN)TRUE, cmdLine.debug);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithTooLongVolIdAndDrive() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 3;
    args->argv = new char *[4];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("VIDVID7");
    args->argv[2] = dupString("111:112:113:114");
    args->argv[3] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE("Testing volume identfier that is too long",
      cmd.parseCmdLine(args->argc, args->argv),
      castor::exception::InvalidArgument);
  }

  void testParceCmdLineWithValidVolIdAndInvalidDrive() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 3;
    args->argv = new char *[4];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("VIDVID");
    args->argv[2] = dupString("INVALID_DRIVE");
    args->argv[3] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Testing valid volume identfier and invalid DRIVE",
      cmd.parseCmdLine(args->argc, args->argv),
      castor::exception::InvalidArgument);
  }

  void testParceCmdLineWithShortTimeout() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("-t");
    args->argv[2] = dupString("2");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing timeout is set after command line is parsed",
      2, cmdLine.timeout);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithLongTimeout() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--timeout");
    args->argv[2] = dupString("2");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing timeout is set after command line is parsed",
      2, cmdLine.timeout);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWith0Timeout() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--timeout");
    args->argv[2] = dupString("0");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Testing valid volume identfier and invalid DRIVE",
      cmd.parseCmdLine(args->argc, args->argv),
      castor::exception::InvalidArgument);
  }

  void testParceCmdLineWithShortQuery() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("-q");
    args->argv[2] = dupString("1");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing query is set after command line is parsed",
      1, cmdLine.queryInterval);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWithLongQuery() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--query");
    args->argv[2] = dupString("1");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    DismountAcsCmdLine cmdLine;
    CPPUNIT_ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Testing query is set after command line is parsed",
      1, cmdLine.queryInterval);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing volume identfier was parsed",
      std::string("VIDVID"), std::string(cmdLine.volId.external_label));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      113, (int)cmdLine.driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      114, (int)cmdLine.driveId.drive);
  }

  void testParceCmdLineWith0Query() {
    Argcv *args = new Argcv();
    m_argsList.push_back(args);
    args->argc = 5;
    args->argv = new char *[6];
    args->argv[0] = dupString("dismountacs");
    args->argv[1] = dupString("--query");
    args->argv[2] = dupString("0");
    args->argv[3] = dupString("VIDVID");
    args->argv[4] = dupString("111:112:113:114");
    args->argv[5] = NULL;

    std::istringstream inStream;
    std::ostringstream outStream;
    std::ostringstream errStream;
    MockAcs acs;
    TestingDismountAcsCmd cmd(inStream, outStream, errStream, acs);
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Testing valid volume identfier and invalid DRIVE",
      cmd.parseCmdLine(args->argc, args->argv),
      castor::exception::InvalidArgument);
  }

  CPPUNIT_TEST_SUITE(DismountAcsCmdTest);
  CPPUNIT_TEST(testParceCmdLineWithNoArgs);
  CPPUNIT_TEST(testParceCmdLineWithOnlyVolId);
  CPPUNIT_TEST(testParceCmdLineWithShortForce);
  CPPUNIT_TEST(testParceCmdLineWithLongForce);
  CPPUNIT_TEST(testParceCmdLineWithShortHelp);
  CPPUNIT_TEST(testParceCmdLineWithLongHelp);
  CPPUNIT_TEST(testParceCmdLineWithVolIdAndDrive);
  CPPUNIT_TEST(testParceCmdLineWithShortDebug);
  CPPUNIT_TEST(testParceCmdLineWithLongDebug);
  CPPUNIT_TEST(testParceCmdLineWithTooLongVolIdAndDrive);
  CPPUNIT_TEST(testParceCmdLineWithValidVolIdAndInvalidDrive);
  CPPUNIT_TEST(testParceCmdLineWithShortTimeout);
  CPPUNIT_TEST(testParceCmdLineWithLongTimeout);
  CPPUNIT_TEST(testParceCmdLineWith0Timeout);
  CPPUNIT_TEST(testParceCmdLineWithShortQuery);
  CPPUNIT_TEST(testParceCmdLineWithLongQuery);
  CPPUNIT_TEST(testParceCmdLineWith0Query);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(DismountAcsCmdTest);

} // namespace mediachanger
} // namespace tape
} // namespace castor
