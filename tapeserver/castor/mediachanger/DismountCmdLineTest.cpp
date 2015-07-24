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

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/DismountCmdLine.hpp"

#include <gtest/gtest.h>
#include <list>
#include <memory>

namespace unitTests {

class castor_mediachanger_DismountCmdLineTest : public ::testing::Test {
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
   * Creates a duplicate string using the new operator.
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
};

TEST_F(castor_mediachanger_DismountCmdLineTest, acs) {
  using namespace castor::mediachanger;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("castor-tape-mediachanger-dismount");
  args->argv[1] = dupString("vid");
  args->argv[2] = dupString("acs1,2,3,4");
  args->argv[3] = NULL;

  std::unique_ptr<DismountCmdLine> cmdLine;
  ASSERT_NO_THROW(cmdLine.reset(new DismountCmdLine(args->argc, args->argv)));

  ASSERT_EQ(false, cmdLine->getHelp());
  ASSERT_EQ(false, cmdLine->getDebug());
  ASSERT_EQ(false, cmdLine->getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine->getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs1,2,3,4"),
    cmdLine->getDriveLibrarySlot().str());
}

TEST_F(castor_mediachanger_DismountCmdLineTest, copy_constructor) {
  using namespace castor::mediachanger;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("castor-tape-mediachanger-dismount");
  args->argv[1] = dupString("vid");
  args->argv[2] = dupString("acs1,2,3,4");
  args->argv[3] = NULL;

  DismountCmdLine cmdLine1(args->argc, args->argv);
  ASSERT_EQ(false, cmdLine1.getHelp());
  ASSERT_EQ(false, cmdLine1.getDebug());
  ASSERT_EQ(false, cmdLine1.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs1,2,3,4"),
    cmdLine1.getDriveLibrarySlot().str());

  DismountCmdLine cmdLine2(cmdLine1);
  ASSERT_EQ(false, cmdLine2.getHelp());
  ASSERT_EQ(false, cmdLine2.getDebug());
  ASSERT_EQ(false, cmdLine2.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs1,2,3,4"),
    cmdLine2.getDriveLibrarySlot().str());
}

TEST_F(castor_mediachanger_DismountCmdLineTest, assignment) {
  using namespace castor::mediachanger;

  Argcv *args1 = new Argcv();
  m_argsList.push_back(args1);
  args1->argc = 3;
  args1->argv = new char *[4];
  args1->argv[0] = dupString("castor-tape-mediachanger-dismount");
  args1->argv[1] = dupString("vid");
  args1->argv[2] = dupString("acs1,2,3,4");
  args1->argv[3] = NULL;

  DismountCmdLine cmdLine1(args1->argc, args1->argv);
  ASSERT_EQ(false, cmdLine1.getHelp());
  ASSERT_EQ(false, cmdLine1.getDebug());
  ASSERT_EQ(false, cmdLine1.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs1,2,3,4"),
    cmdLine1.getDriveLibrarySlot().str());

  Argcv *args2 = new Argcv();
  m_argsList.push_back(args2);
  args2->argc = 3;
  args2->argv = new char *[4];
  args2->argv[0] = dupString("castor-tape-mediachanger-dismount");
  args2->argv[1] = dupString("vid");
  args2->argv[2] = dupString("acs5,6,7,8");
  args2->argv[3] = NULL;

  DismountCmdLine cmdLine2(args2->argc, args2->argv);
  ASSERT_EQ(false, cmdLine2.getHelp());
  ASSERT_EQ(false, cmdLine2.getDebug());
  ASSERT_EQ(false, cmdLine2.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs5,6,7,8"),
    cmdLine2.getDriveLibrarySlot().str());

  cmdLine1 = cmdLine2;

  ASSERT_EQ(false, cmdLine1.getHelp());
  ASSERT_EQ(false, cmdLine1.getDebug());
  ASSERT_EQ(false, cmdLine1.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs5,6,7,8"),
    cmdLine1.getDriveLibrarySlot().str());

  ASSERT_EQ(false, cmdLine2.getHelp());
  ASSERT_EQ(false, cmdLine2.getDebug());
  ASSERT_EQ(false, cmdLine2.getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS,
    cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("acs5,6,7,8"),
    cmdLine2.getDriveLibrarySlot().str());
}

TEST_F(castor_mediachanger_DismountCmdLineTest, scsi) {
  using namespace castor::mediachanger;
  
  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4]; 
  args->argv[0] = dupString("castor-tape-mediachanger-dismount");
  args->argv[1] = dupString("vid");
  args->argv[2] = dupString("smc@rmcHostname,1");
  args->argv[3] = NULL;
  
  std::unique_ptr<DismountCmdLine> cmdLine;
  ASSERT_NO_THROW(cmdLine.reset(new DismountCmdLine(args->argc, args->argv)));

  ASSERT_EQ(false, cmdLine->getHelp());
  ASSERT_EQ(false, cmdLine->getDebug());
  ASSERT_EQ(false, cmdLine->getForce());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI,
    cmdLine->getDriveLibrarySlot().getLibraryType());
}

} // namespace unitTests
