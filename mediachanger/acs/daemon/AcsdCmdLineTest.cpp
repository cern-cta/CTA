/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "mediachanger/acs/daemon/AcsdCmdLine.hpp"
#include <gtest/gtest.h>
#include <list>
#include <memory>

namespace unitTests {

class cta_mediachanger_acs_daemon_AcsdCmdLineTest : public ::testing::Test {
protected:

  struct Argcv {
    int argc;
    char **argv;
    Argcv(): argc(0), argv(NULL) {
    }
  };
  typedef std::list<Argcv*> ArgcvList;
  ArgcvList m_argsList;

  /**
   * Creates a duplicate string using the new operator.
   */
  char *dupString(const char *str) {
    const size_t len = strlen(str);
    char *duplicate = new char[len+1];
    strncpy(duplicate, str, len);
    duplicate[len] = '\0';
    return duplicate;
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
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


TEST_F(cta_mediachanger_acs_daemon_AcsdCmdLineTest, constructor_no_command_line_args) {
  using namespace cta::mediachanger::acs::daemon;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 1;
  args->argv = new char *[2];
  args->argv[0] = dupString("cta-acsd");
  args->argv[1] = NULL;
  cta::mediachanger::acs::daemon::AcsdCmdLine acsdcmdLine1(args->argc, args->argv);
 
  ASSERT_FALSE(acsdcmdLine1.foreground);
  ASSERT_TRUE(acsdcmdLine1.configLocation.empty());
  ASSERT_FALSE(acsdcmdLine1.help);
}

TEST_F(cta_mediachanger_acs_daemon_AcsdCmdLineTest, default_constructor) {
  using namespace cta::mediachanger::acs::daemon;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 0;
  cta::mediachanger::acs::daemon::AcsdCmdLine acsdcmdLine1(args->argc, args->argv);
 
  ASSERT_FALSE(acsdcmdLine1.foreground);
  ASSERT_TRUE(acsdcmdLine1.configLocation.empty());
  ASSERT_FALSE(acsdcmdLine1.help);
}

TEST_F(cta_mediachanger_acs_daemon_AcsdCmdLineTest, constructor_foreground) {
  using namespace cta::mediachanger::acs::daemon;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-acsd");
  args->argv[1] = dupString("-f");
  args->argv[2] = NULL;
  cta::mediachanger::acs::daemon::AcsdCmdLine acsdcmdLine1(args->argc, args->argv);
 
  ASSERT_TRUE(acsdcmdLine1.foreground);
}


TEST_F(cta_mediachanger_acs_daemon_AcsdCmdLineTest, constructor_help) {
  using namespace cta::mediachanger::acs::daemon;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-mediachanger-acs-daemon");
  args->argv[1] = dupString("-h");
  args->argv[2] = NULL;
  cta::mediachanger::acs::daemon::AcsdCmdLine acsdcmdLine2(args->argc, args->argv);
  ASSERT_TRUE(acsdcmdLine2.help);
}

 
TEST_F(cta_mediachanger_acs_daemon_AcsdCmdLineTest, constructor_configLocation) {
  using namespace cta::mediachanger::acs::daemon;

  Argcv *args1 = new Argcv();
  m_argsList.push_back(args1);
  args1->argc = 2;
  args1->argv = new char *[3];
  args1->argv[0] = dupString("cta-mediachanger-acs-daemon");
  args1->argv[1] = dupString("-c");
  args1->argv[2] = NULL;
  cta::mediachanger::acs::daemon::AcsdCmdLine acsdcmdLine3(args1->argc, args1->argv);
  ASSERT_EQ(acsdcmdLine3.configLocation,"");
}

} // namespace unitTests
