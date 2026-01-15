/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CreateAdminUserCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_CreateAdminUserCmdLineArgsTest : public ::testing::Test {
protected:
  struct Argcv {
    int argc;
    char** argv;

    Argcv() : argc(0), argv(nullptr) {}
  };

  using ArgcvVect = std::vector<Argcv*>;
  ArgcvVect m_args;

  /**
   * Creates a duplicate string using the new operator.
   */
  char* dupString(const std::string& str) {
    const int len = str.size();
    char* copy = new char[len + 1];
    std::copy(str.begin(), str.end(), copy);
    copy[len] = '\0';
    return copy;
  }

  virtual void SetUp() {
    // Allow getopt_long to be called again
    optind = 0;
  }

  virtual void TearDown() {
    // Allow getopt_long to be called again
    optind = 0;

    for (ArgcvVect::const_iterator itor = m_args.begin(); itor != m_args.end(); itor++) {
      for (int i = 0; i < (*itor)->argc; i++) {
        delete[] (*itor)->argv[i];
      }
      delete[] (*itor)->argv;
      delete *itor;
    }
  }
};

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_args.push_back(args);
  args->argc = 2;
  args->argv = new char*[3];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("-h");
  args->argv[2] = nullptr;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_args.push_back(args);
  args->argc = 2;
  args->argv = new char*[3];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("--help");
  args->argv[2] = nullptr;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_short) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_args.push_back(args);
  args->argc = 6;
  args->argv = new char*[7];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("-u");
  args->argv[3] = dupString("adminUsername");
  args->argv[4] = dupString("-m");
  args->argv[5] = dupString("comment");
  args->argv[6] = nullptr;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_long) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_args.push_back(args);
  args->argc = 6;
  args->argv = new char*[7];
  args->argv[0] = dupString("cta-catalogue-admin-user-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("--username");
  args->argv[3] = dupString("adminUsername");
  args->argv[4] = dupString("--comment");
  args->argv[5] = dupString("comment");
  args->argv[6] = nullptr;

  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

}  // namespace unitTests
