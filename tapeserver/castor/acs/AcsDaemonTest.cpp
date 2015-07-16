/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


#include "castor/log/DummyLogger.hpp"
#include "castor/server/ProcessCapDummy.hpp"
#include "castor/acs/AcsDaemon.hpp"

#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <sstream>

namespace unitTests {

class castor_acs_AcsDaemonTest : public ::testing::Test {
protected:
  void *const m_zmqContext;

  castor_acs_AcsDaemonTest(): m_zmqContext(zmq_init(1)) {
  }

  ~castor_acs_AcsDaemonTest() {
    if(NULL != m_zmqContext) {
      zmq_term(m_zmqContext);
    }
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class castor_acs_AcsDaemonTest

TEST_F(castor_acs_AcsDaemonTest, zmqContext) {
  ASSERT_NE((void *const)0, m_zmqContext);
}

TEST_F(castor_acs_AcsDaemonTest, constructor) {
  using namespace castor::acs;

  int argc = 1;
  char argv0[1024];
  memset(argv0, '\0', sizeof(argv0));
  char *argv[2] = {argv0, NULL};
  std::ostringstream stdOut;
  std::ostringstream stdErr;
  const std::string programName = "unittests";
  castor::log::DummyLogger log(programName);
    
  castor::tape::reactor::ZMQReactor reactor(log);
  std::auto_ptr<AcsDaemon> daemon;
  const AcsDaemonConfig config;
  ASSERT_NO_THROW(daemon.reset(new AcsDaemon(argc, argv, stdOut, stdErr, log,
    reactor, config)));
}

} // namespace unitTests
