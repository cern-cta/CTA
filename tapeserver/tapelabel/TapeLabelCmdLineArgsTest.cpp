/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "tapeserver/tapelabel/TapeLabelCmdLineArgs.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest : public ::testing::Test {
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
    // Allow getopt_long to be called again
    optind = 0;
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

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, help_short) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-h");
  args->argv[2] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, help_long) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("--help");
  args->argv[2] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, debug_short) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("-d");
  args->argv[4] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_debug);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, debug_long) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("--debug");
  args->argv[4] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_debug);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, force_short) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("-f");
  args->argv[4] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_force);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, force_long) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("--force");
  args->argv[4] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_force);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_short) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_long) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 3;
  args->argv = new char *[4];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("--vid");
  args->argv[2] = dupString("VID001");
  args->argv[3] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_missed) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 2;
  args->argv = new char *[3];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("--vid");
  args->argv[2] = NULL;

  ASSERT_THROW(TapeLabelCmdLineArgs cmdLine(args->argc, args->argv),
    cta::exception::CommandLineNotParsed);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_short) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("-o");
  args->argv[4] = dupString("VID002");
  args->argv[5] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
  ASSERT_EQ(std::string("VID002"), cmdLine.m_oldLabel);
}


TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_long) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 5;
  args->argv = new char *[6];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("--oldlabel");
  args->argv[4] = dupString("VID002");
  args->argv[5] = NULL;

  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
  ASSERT_EQ(std::string("VID002"), cmdLine.m_oldLabel);
}


TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_missed) {
  using namespace cta::tapeserver::tapelabel;

  Argcv *args = new Argcv();
  m_argsList.push_back(args);
  args->argc = 4;
  args->argv = new char *[5];
  args->argv[0] = dupString("cta-tape-label");
  args->argv[1] = dupString("-v");
  args->argv[2] = dupString("VID001");
  args->argv[3] = dupString("-o");
  args->argv[4] = NULL;
  
  ASSERT_THROW(TapeLabelCmdLineArgs cmdLine(args->argc, args->argv),
    cta::exception::CommandLineNotParsed);
}

} // namespace unitTests
