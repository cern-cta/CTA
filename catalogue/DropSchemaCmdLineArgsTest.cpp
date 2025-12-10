/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Argcv.hpp"
#include "common/exception/Exception.hpp"
#include "catalogue/DropSchemaCmdLineArgs.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_DropSchemaCmdLineArgsTest : public ::testing::Test {
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

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-drop", "-h");
  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-drop", "--help");
  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, dbConfigPath) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-drop", "dbConfigPath");
  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
}

}  // namespace unitTests
