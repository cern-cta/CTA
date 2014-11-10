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

#include "castor/acs/AcsMountCmdLine.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"

#include <gtest/gtest.h>
#include <list>
#include <memory>
#include <sstream>
#include <string.h>

namespace unitTests {

class castor_acs_AcsMountCmdLineTest: public ::testing::Test {
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
}; // class castor_acs_AcsMountCmdLineTest

TEST_F(castor_acs_AcsMountCmdLineTest, constructor) {
  const castor::acs::AcsMountCmdLine cmdLine;
  ASSERT_EQ(false, cmdLine.debug);
  ASSERT_EQ(false, cmdLine.help);
  ASSERT_EQ((BOOLEAN)FALSE, cmdLine.readOnly);
  ASSERT_EQ(0, cmdLine.timeout);
  ASSERT_EQ(0, cmdLine.queryInterval);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(0, (int)cmdLine.driveId.drive);
  ASSERT_EQ('\0', cmdLine.volId.external_label[0]);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithNoArgs) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 1;
  args->argv = new char *[2];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout), castor::exception::MissingOperand);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithOnlyVolId) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("VIDVID");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout), castor::exception::MissingOperand);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithShortReadOnly) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("-r");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = dupString("111:112:113:114");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.readOnly);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithLongReadOnly) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--readonly");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = dupString("111:112:113:114");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ((BOOLEAN)TRUE, cmdLine.readOnly);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithShortHelp) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("-h");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(true, cmdLine.help);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithLongHelp) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--help");
  args->argv[2] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(true, cmdLine.help);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithVolIdAndDrive) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("VIDVID");
  args->argv[2] = dupString("111:112:113:114");
  args->argv[3] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(false, cmdLine.debug);
  ASSERT_EQ(false, cmdLine.help);
  ASSERT_EQ(defaultQueryInterval, cmdLine.queryInterval);
  ASSERT_EQ(defaultTimeout, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithShortDebug) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("-d");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = dupString("111:112:113:114");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(true, cmdLine.debug);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithLongDebug) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--debug");
  args->argv[2] = dupString("VIDVID");
  args->argv[3] = dupString("111:112:113:114");
  args->argv[4] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(true, cmdLine.debug);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithTooLongVolIdAndDrive) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("VIDVID7");
  args->argv[2] = dupString("111:112:113:114");
  args->argv[3] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout),
    castor::exception::InvalidArgument);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithValidVolIdAndInvalidDrive) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("VIDVID");
  args->argv[2] = dupString("INVALID_DRIVE");
  args->argv[3] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout),
    castor::exception::InvalidArgument);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithShortTimeout) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("-t");
  args->argv[2] = dupString("2");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(2, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithLongTimeout) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--timeout");
  args->argv[2] = dupString("2");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(2, cmdLine.timeout);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWith0Timeout) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--timeout");
  args->argv[2] = dupString("0");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout),
    castor::exception::InvalidArgument);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithShortQuery) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("-q");
  args->argv[2] = dupString("1");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(1, cmdLine.queryInterval);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWithLongQuery) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--query");
  args->argv[2] = dupString("1");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  AcsMountCmdLine cmdLine;
  ASSERT_NO_THROW(cmdLine = AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout));
  ASSERT_EQ(1, cmdLine.queryInterval);
  ASSERT_EQ(std::string("VIDVID"), std::string(cmdLine.volId.external_label));
  ASSERT_EQ(111, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(112, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(113, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(114, (int)cmdLine.driveId.drive);
}

TEST_F(castor_acs_AcsMountCmdLineTest, parseCmdLineWith0Query) {
  using namespace castor::acs;
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("mountacs");
  args->argv[1] = dupString("--query");
  args->argv[2] = dupString("0");
  args->argv[3] = dupString("VIDVID");
  args->argv[4] = dupString("111:112:113:114");
  args->argv[5] = NULL;

  std::istringstream inStream;
  std::ostringstream outStream;
  std::ostringstream errStream;
  const int defaultQueryInterval = 10;
  const int defaultTimeout = 20;
  ASSERT_THROW(AcsMountCmdLine(args->argc, args->argv,
    defaultQueryInterval, defaultTimeout),
    castor::exception::InvalidArgument);
}

} // namespace unitTests
