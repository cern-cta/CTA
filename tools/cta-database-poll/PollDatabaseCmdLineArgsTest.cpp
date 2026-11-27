/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "PollDatabaseCmdLineArgs.hpp"

#include "tests/support/Argv.hpp"

#include <getopt.h>
#include <gtest/gtest.h>

namespace unitTests {

class cta_catalogue_PollDatabaseCmdLineArgsTest : public ::testing::Test {
protected:
  // Each test parses a fresh command line with getopt_long.
  void SetUp() override { optind = 0; }

  void TearDown() override { optind = 0; }
};

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, help_short) {
  Argv args {"cta-database-poll", "-h"};
  const cta::catalogue::PollDatabaseCmdLineArgs cmdLine(args.count, args.data());

  ASSERT_TRUE(cmdLine.help);
}

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, help_long) {
  Argv args {"cta-database-poll", "--help"};
  const cta::catalogue::PollDatabaseCmdLineArgs cmdLine(args.count, args.data());

  ASSERT_TRUE(cmdLine.help);
}

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, all_args) {
  Argv args {"cta-database-poll", "dbConfigPath", "1234"};
  const cta::catalogue::PollDatabaseCmdLineArgs cmdLine(args.count, args.data());

  ASSERT_FALSE(cmdLine.help);
  ASSERT_FALSE(cmdLine.untilReady);
  ASSERT_EQ("dbConfigPath", cmdLine.dbConfigPath);
  ASSERT_EQ(1234, cmdLine.numberOfSecondsToKeepPolling);
}

TEST_F(cta_catalogue_PollDatabaseCmdLineArgsTest, until_ready) {
  Argv args {"cta-database-poll", "--until-ready", "scheduler.conf", "120"};
  const cta::catalogue::PollDatabaseCmdLineArgs cmdLine(args.count, args.data());

  ASSERT_TRUE(cmdLine.untilReady);
  ASSERT_EQ("scheduler.conf", cmdLine.dbConfigPath);
  ASSERT_EQ(120, cmdLine.numberOfSecondsToKeepPolling);
}

}  // namespace unitTests
