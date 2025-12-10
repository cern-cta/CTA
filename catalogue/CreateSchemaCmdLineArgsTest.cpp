/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Argcv.hpp"
#include "common/exception/Exception.hpp"
#include "catalogue/CreateSchemaCmdLineArgs.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_CreateSchemaCmdLineArgsTest : public ::testing::Test {
protected:

  virtual void SetUp() override {
    // Allow getopt_long to be called again
    optind = 0;
  }

  void TearDown() override {
    // Allow getopt_long to be called again
    optind = 0;
  }
};

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-create", "-h");
  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-create", "--help");
  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, version_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-create", "dbConfigPath", "-v", "4.5");
  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(cmdLine.catalogueVersion.value(), "4.5");
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, version_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-create", "dbConfigPath", "--version", "4.5");
  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(cmdLine.catalogueVersion.value(), "4.5");
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, dbConfigPath) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-create", "dbConfigPath");
  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
}

}  // namespace unitTests
