/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "catalogue/CreateSchemaCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_catalogue_CreateSchemaCmdLineArgsTest : public ::testing::Test {
protected:

  struct Argcv {
    int argc;
    char **argv;
    Argcv(): argc(0), argv(nullptr) {
    }
  };
  using ArgcvList = std::list<Argcv*>;
  ArgcvList m_argsList;

  /**
   * Creates a duplicate string using the new operator.
   */
  char *dupString(const std::string &str) {
    const int len = str.size();
    char *copy = new char[len + 1];
    std::copy(str.begin(), str.end(), copy);
    copy[len] = '\0';
    return copy;
  }

  virtual void SetUp() override {
    // Allow getopt_long to be called again
    optind = 0;
  }

  void TearDown() override {
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

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, help_short) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-catalogue-schema-create");
  args->argv[1] = dupString("-h");
  args->argv[2] = nullptr;

  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, help_long) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-catalogue-schema-create");
  args->argv[1] = dupString("--help");
  args->argv[2] = nullptr;

  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.dbConfigPath.empty());
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, version_short) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-catalogue-schema-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("-v");
  args->argv[3] = dupString("4.5");
  args->argv[4] = nullptr;

  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(cmdLine.catalogueVersion.value(), "4.5");
}

TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, version_long) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-catalogue-schema-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = dupString("--version");
  args->argv[3] = dupString("4.5");
  args->argv[4] = nullptr;

  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(cmdLine.catalogueVersion.value(), "4.5");
}


TEST_F(cta_catalogue_CreateSchemaCmdLineArgsTest, dbConfigPath) {
  using namespace cta::catalogue;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-catalogue-schema-create");
  args->argv[1] = dupString("dbConfigPath");
  args->argv[2] = nullptr;

  CreateSchemaCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("dbConfigPath"), cmdLine.dbConfigPath);
}

} // namespace unitTests
