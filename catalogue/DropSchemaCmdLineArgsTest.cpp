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
#include "catalogue/DropSchemaCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_DropSchemaCmdLineArgsTest : public ::testing::Test {
protected:
  struct Argcv {
    int argc;
    char** argv;

    Argcv() : argc(0), argv(nullptr) {}
  };

  typedef std::list<Argcv*> ArgcvList;
  ArgcvList m_argsList;

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

    for (ArgcvList::const_iterator itor = m_argsList.begin(); itor != m_argsList.end(); itor++) {
      for (int i = 0; i < (*itor)->argc; i++) {
        delete[] (*itor)->argv[i];
      }
      delete[] (*itor)->argv;
      delete *itor;
    }
  }
};

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char*[3];
  args->argv[0] = dupString("cta-catalogue-schema-drop");
  args->argv[1] = dupString("-h");
  args->argv[2] = nullptr;

  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char*[3];
  args->argv[0] = dupString("cta-catalogue-schema-drop");
  args->argv[1] = dupString("--help");
  args->argv[2] = nullptr;

  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_DropSchemaCmdLineArgsTest, dbConfigPath) {
  using namespace cta::catalogue;

  Argcv* args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char*[3];
  args->argv[0] = dupString("cta-catalogue-schema-drop");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = nullptr;

  DropSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
}

}  // namespace unitTests
