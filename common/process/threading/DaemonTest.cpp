/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/Daemon.hpp"

#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>
#include <sstream>
#include <stdio.h>
#include <string.h>

namespace unitTests {

class cta_threading_DaemonTest : public ::testing::Test {
protected:
  const std::string m_hostName;
  const std::string m_programName;
  int m_argc;
  char** m_argv;

  cta_threading_DaemonTest() : m_hostName("dummy"), m_programName("testdaemon"), m_argc(0), m_argv(nullptr) {}

  virtual void SetUp() {
    m_argc = 0;
    m_argv = nullptr;
  }

  virtual void TearDown() {
    for (int i = 0; i < m_argc; i++) {
      free(m_argv[i]);
    }

    delete[] m_argv;
  }
};

TEST_F(cta_threading_DaemonTest, getForegroundBeforeParseCommandLine) {
  cta::log::DummyLogger log(m_hostName, m_programName);
  cta::server::Daemon daemon(log);

  ASSERT_THROW(daemon.getForeground(), cta::server::Daemon::CommandLineNotParsed);
}

}  // namespace unitTests
