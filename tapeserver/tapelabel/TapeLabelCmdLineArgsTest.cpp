/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Argcv.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "tapeserver/tapelabel/TapeLabelCmdLineArgs.hpp"

#include "common/exception/CommandLineNotParsed.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <list>

namespace unitTests {

class cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest : public ::testing::Test {
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

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, help_short) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-h");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, help_long) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "--help");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.help);
  ASSERT_TRUE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, debug_short) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "-d");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_debug);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, debug_long) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "--debug");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_debug);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, force_short) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "-f");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_force);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, force_long) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "--force");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_TRUE(cmdLine.m_force);
  ASSERT_FALSE(cmdLine.m_vid.empty());
  ASSERT_TRUE(cmdLine.m_oldLabel.empty());
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_short) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_long) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "--vid", "VID001");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, vid_missed) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "--vid");

  ASSERT_THROW(TapeLabelCmdLineArgs cmdLine(args->argc, args->argv), cta::exception::CommandLineNotParsed);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_short) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "-o", "VID002");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
  ASSERT_EQ(std::string("VID002"), cmdLine.m_oldLabel);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_long) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "--oldlabel", "VID002");
  TapeLabelCmdLineArgs cmdLine(args->argc, args->argv);

  ASSERT_FALSE(cmdLine.help);
  ASSERT_EQ(std::string("VID001"), cmdLine.m_vid);
  ASSERT_EQ(std::string("VID002"), cmdLine.m_oldLabel);
}

TEST_F(cta_tapeserver_tapelabel_TapeLabelCmdLineArgsTest, oldVid_missed) {
  using namespace cta::tapeserver::tapelabel;

  auto args = std::make_unique<Argcv>("cta-tape-label", "-v", "VID001", "-o");
  ASSERT_THROW(TapeLabelCmdLineArgs cmdLine(args->argc, args->argv),
    cta::exception::CommandLineNotParsed);
}

}  // namespace unitTests
