/******************************************************************************
 *         castor/tape/tapeserver/daemon/RmcImplTest.cpp
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

#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/RmcImpl.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_RmcImplTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_RmcImplTest, getDriveType) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string programName = "unittests";
  castor::log::DummyLogger log(programName);
  const std::string rmcHostName = "";
  const unsigned short rmcPort = 0;
  const int netTimeout = 1; // Timeout in seconds
  
  RmcImpl rmc(log, rmcHostName, rmcPort, netTimeout);

  ASSERT_EQ(RmcImpl::RMC_DRIVE_TYPE_ACS, rmc.getDriveType("acs@rmc_host,1,2,3,4"));
  ASSERT_EQ(RmcImpl::RMC_DRIVE_TYPE_MANUAL, rmc.getDriveType("manual"));
  ASSERT_EQ(RmcImpl::RMC_DRIVE_TYPE_SCSI, rmc.getDriveType("smc@rmc_host,1"));
  ASSERT_EQ(RmcImpl::RMC_DRIVE_TYPE_UNKNOWN, rmc.getDriveType("nonsense"));
}

} // namespace unitTests
