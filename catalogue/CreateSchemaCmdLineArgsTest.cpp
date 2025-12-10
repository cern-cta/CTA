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

#include "common/Argcv.hpp"
#include "common/exception/Exception.hpp"
#include "catalogue/CreateSchemaCmdLineArgs.hpp"

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

} // namespace unitTests
