/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Argcv.hpp"
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
  }
};

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-admin-user-create", "-h");
  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-admin-user-create", "--help");
  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_TRUE(cmdLine.adminUsername.empty());
  ASSERT_TRUE(cmdLine.comment.empty());
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-admin-user-create", "dbConfigPath", "-u", "adminUsername", "-m", "comment");
  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

TEST_F(cta_catalogue_CreateAdminUserCmdLineArgsTest, username_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-admin-user-create", "dbConfigPath", "--username", "adminUsername", "--comment", "comment");
  CreateAdminUserCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(std::string("adminUsername"), cmdLine.adminUsername);
  ASSERT_EQ(std::string("comment"), cmdLine.comment);
}

}  // namespace unitTests
