/******************************************************************************
 *         castor/tape/tapeserver/daemon/TapeDaemonTest.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/io/DummyPollReactor.hpp"
#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/DummyVdqm.hpp"
#include "castor/tape/tapeserver/daemon/TestingTapeDaemon.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>
#include <iostream>

namespace unitTests {

class castor_tape_tapeserver_daemon_TapeDaemonTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_TapeDaemonTest, checkGoodTpconfigDgns) {
  std::ostringstream stdOut;
  std::ostringstream stdErr;
  const std::string programName = "unittests";
  castor::log::DummyLogger log(programName);
  castor::tape::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  castor::tape::tapeserver::daemon::DummyVdqm vdqm(job);
  castor::io::DummyPollReactor reactor;
  castor::tape::tapeserver::daemon::TestingTapeDaemon
    daemon(stdOut, stdErr, log, vdqm, reactor);

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN", "down", "CONTROLMETHOD", "DEVTYPE"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN", "DEV", "DEN", "down", "CONTROLMETHOD", "DEVTYPE"));
  ASSERT_NO_THROW(daemon.checkTpconfigDgns(lines));
}
