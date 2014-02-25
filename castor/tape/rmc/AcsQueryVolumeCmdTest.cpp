/******************************************************************************
 *           castor/tape/rmc/AcsQueryVolumeCmdTest.hpp
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

#include "castor/tape/rmc/MockAcs.hpp"
#include "castor/tape/rmc/TestingAcsQueryVolumeCmd.hpp"

#include <gtest/gtest.h>
#include <list>
#include <memory>
#include <sstream>
#include <string.h>

namespace unitTests {

class castor_tape_rmc_AcsQueryVolumeCmdTest: public ::testing::Test {
protected:

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

  virtual void SetUp() {
  }

  virtual void TearDown() {
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
}; // class castor_tape_rmc_AcsQueryVolumeCmdTest

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithNoArgs) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 1;
  args->argv = new char *[2];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  ASSERT_THROW(cmd.parseCmdLine(args->argc, args->argv),
    castor::exception::MissingOperand);
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithValidVolId) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("VIDVID");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ((BOOLEAN)FALSE, cmdLine.debug);
  ASSERT_EQ((BOOLEAN)FALSE, cmdLine.help);
  ASSERT_EQ(1, cmdLine.queryInterval);
  ASSERT_EQ(20, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithShortHelp) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("-h");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.help);
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithLongHelp) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--help");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.help);
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithShortDebug) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("-d");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.debug);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithLongDebug) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--debug");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.debug);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithTooLongVolId) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("VIDVID7");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  ASSERT_THROW(cmd.parseCmdLine(args->argc, args->argv),
    castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithShortTimeout) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("-t");
  args->argv[2] = dupString("2");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ(2, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithLongTimeout) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--timeout");
  args->argv[2] = dupString("2");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ(2, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWith0Timeout) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--timeout");
  args->argv[2] = dupString("0");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  ASSERT_THROW(cmd.parseCmdLine(args->argc, args->argv),
    castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithShortQuery) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("-q");
  args->argv[2] = dupString("1");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ(1, cmdLine.queryInterval);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWithLongQuery) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--query");
  args->argv[2] = dupString("1");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  AcsQueryVolumeCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = cmd.parseCmdLine(args->argc, args->argv));
  ASSERT_EQ(1, cmdLine.queryInterval);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
}

TEST_F(castor_tape_rmc_AcsQueryVolumeCmdTest, parceCmdLineWith0Query) {
  using namespace castor::tape::rmc;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("queryvolume");
  args->argv[1] = dupString("--query");
  args->argv[2] = dupString("0");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  MockAcs acs;
  TestingAcsQueryVolumeCmd cmd(inStream, outStream, errStream, acs);
  ASSERT_THROW(cmd.parseCmdLine(args->argc, args->argv),
    castor::exception::InvalidArgument);
}

} // namespace unitTests
