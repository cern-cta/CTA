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
#include "catalogue/VerifySchemaCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_VerifySchemaCmdLineArgsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    // Allow getopt_long to be called again
    optind = 0;
  }

  virtual void TearDown() {
    // Allow getopt_long to be called again
    optind = 0;
  }
};

TEST_F(cta_catalogue_VerifySchemaCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-verify", "-h");
  VerifySchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_VerifySchemaCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-verify", "--help");
  VerifySchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_VerifySchemaCmdLineArgsTest, dbConfigPath) {
  using namespace cta::catalogue;

  auto args = std::make_unique<Argcv>("cta-catalogue-schema-verify", "dbConfigPath");
  VerifySchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
}

} // namespace unitTests
