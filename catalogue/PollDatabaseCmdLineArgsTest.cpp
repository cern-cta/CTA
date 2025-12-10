/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Argcv.hpp"
#include "common/exception/Exception.hpp"
#include "catalogue/PollDatabaseCmdLineArgs.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_PollDatabaseCmdLineArgsTest : public ::testing::Test {
protected:

  void SetUp() override {
    // Allow getopt_long to be called again
    optind = 0;
  }

  void TearDown() override {
    // Allow getopt_long to be called again
    optind = 0;
  }
};

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-database-poll", "-h");
  PollDatabaseCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_EQ(0, cmdLine.numberOfSecondsToKeepPolling);
}

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-database-poll", "--help");
  PollDatabaseCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
  ASSERT_EQ(0, cmdLine.numberOfSecondsToKeepPolling);
}

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, all_args) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-database-poll", "dbConfigPath", "1234");
  PollDatabaseCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
  ASSERT_EQ(1234, cmdLine.numberOfSecondsToKeepPolling);
}

}  // namespace unitTests
