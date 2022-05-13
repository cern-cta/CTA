/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "mediachanger/MountCmdLine.hpp"

#include <gtest/gtest.h>
#include <list>
#include <memory>

namespace unitTests {

class cta_mediachanger_MountCmdLineTest : public ::testing::Test {
protected:

  struct Argcv {
    int argc;
    char **argv;
    Argcv(): argc(0), argv(nullptr) {
    }
  };
  typedef std::list<Argcv*> ArgcvList;
  ArgcvList m_argsList;

  /**
   * Creates a duplicate string using the new operator.
   */
  char *dupString(const std::string &str) {
    const int len = str.size();
    char *copy = new char[len + 1];
    std::copy(str.begin(), str.end(), copy);
    copy[len] = '\0';
    return copy;
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

TEST_F(cta_mediachanger_MountCmdLineTest, copy_constructor) {
  using namespace cta::mediachanger;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("cta-mediachanger-mount");
  args->argv[1] = dupString("vid");
  args->argv[2] = dupString("smc1");
  args->argv[3] = nullptr;

  MountCmdLine cmdLine1(args->argc, args->argv);
  ASSERT_FALSE(cmdLine1.getHelp());
  ASSERT_FALSE(cmdLine1.getDebug());
  ASSERT_FALSE(cmdLine1.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc1"), cmdLine1.getDriveLibrarySlot().str());

  MountCmdLine cmdLine2(cmdLine1);
  ASSERT_FALSE(cmdLine2.getHelp());
  ASSERT_FALSE(cmdLine2.getDebug());
  ASSERT_FALSE(cmdLine2.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc1"), cmdLine2.getDriveLibrarySlot().str());
}

TEST_F(cta_mediachanger_MountCmdLineTest, assignment) {
  using namespace cta::mediachanger;

  Argcv *args1 = new Argcv();
  m_argsList.push_back(args1);
  args1->argc = 3;
  args1->argv = new char *[4];
  args1->argv[0] = dupString("cta-mediachanger-mount");
  args1->argv[1] = dupString("vid");
  args1->argv[2] = dupString("smc1");
  args1->argv[3] = nullptr;

  MountCmdLine cmdLine1(args1->argc, args1->argv);
  ASSERT_FALSE(cmdLine1.getHelp());
  ASSERT_FALSE(cmdLine1.getDebug());
  ASSERT_FALSE(cmdLine1.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc1"), cmdLine1.getDriveLibrarySlot().str());

  Argcv *args2 = new Argcv();
  m_argsList.push_back(args2);
  args2->argc = 3;
  args2->argv = new char *[4];
  args2->argv[0] = dupString("cta-mediachanger-mount");
  args2->argv[1] = dupString("vid");
  args2->argv[2] = dupString("smc2");
  args2->argv[3] = nullptr;

  MountCmdLine cmdLine2(args2->argc, args2->argv);
  ASSERT_FALSE(cmdLine2.getHelp());
  ASSERT_FALSE(cmdLine2.getDebug());
  ASSERT_FALSE(cmdLine2.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc2"), cmdLine2.getDriveLibrarySlot().str());

  cmdLine1 = cmdLine2;

  ASSERT_FALSE(cmdLine1.getHelp());
  ASSERT_FALSE(cmdLine1.getDebug());
  ASSERT_FALSE(cmdLine1.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine1.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc2"), cmdLine1.getDriveLibrarySlot().str());

  ASSERT_FALSE(cmdLine2.getHelp());
  ASSERT_FALSE(cmdLine2.getDebug());
  ASSERT_FALSE(cmdLine2.getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine2.getDriveLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("smc2"), cmdLine2.getDriveLibrarySlot().str());
}

TEST_F(cta_mediachanger_MountCmdLineTest, scsi) {
  using namespace cta::mediachanger;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("cta-mediachanger-mount");
  args->argv[1] = dupString("vid");
  args->argv[2] = dupString("smc1");
  args->argv[3] = nullptr;

  std::unique_ptr<MountCmdLine> cmdLine;
  ASSERT_NO_THROW(cmdLine.reset(new MountCmdLine(args->argc, args->argv)));

  ASSERT_FALSE(cmdLine->getHelp());
  ASSERT_FALSE(cmdLine->getDebug());
  ASSERT_FALSE(cmdLine->getReadOnly());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, cmdLine->getDriveLibrarySlot().getLibraryType());
}

} // namespace unitTests
