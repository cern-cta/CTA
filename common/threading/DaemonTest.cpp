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

#include "common/log/DummyLogger.hpp"
#include "common/threading/Daemon.hpp"

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
  char **m_argv;

  cta_threading_DaemonTest() :
    m_hostName("dummy"),
    m_programName("testdaemon"),
    m_argc(0),
    m_argv(NULL) {
  }

  virtual void SetUp() {
    m_argc = 0;
    m_argv = NULL;
  }

  virtual void TearDown() {
    for(int i = 0; i < m_argc; i++) {
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

} // namespace unitTests
