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

#include "catalogue/CreateAdminUserCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_CreateAdminUserCmdLineArgsTest : public ::testing::Test {
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
    // Allow getopt_long to be called again
    optind = 0;
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

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("-h");
  args->argv[2] = NULL;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("--help");
  args->argv[2] = NULL;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_short) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 6;
  args->argv = new char *[7];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("-u");
  args->argv[3] = dupString("adminUsername");
  args->argv[4] = dupString("-m");
  args->argv[5] = dupString("comment");
  args->argv[6] = NULL;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_long) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 6;
  args->argv = new char *[7];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("--username");
  args->argv[3] = dupString("adminUsername");
  args->argv[4] = dupString("--comment");
  args->argv[5] = dupString("comment");
  args->argv[6] = NULL;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

} // namespace unitTests
