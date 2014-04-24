/******************************************************************************
 *         castor/tape/tapeserver/daemon/RmcProxyImplTest.cpp
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
#include "castor/legacymsg/RmcProxyImpl.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_RmcProxyImplTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_RmcProxyImplTest, getLibrarySlotType) {
  using namespace castor::legacymsg;

  const std::string programName = "unittests";
  castor::log::DummyLogger log(programName);
  const int netTimeout = 1; // Timeout in seconds
  
  RmcProxyImpl rmc(log, netTimeout);

  ASSERT_EQ(RmcProxyImpl::RMC_LIBRARY_SLOT_TYPE_ACS, rmc.getLibrarySlotType("acs@rmc_host,1,2,3,4"));
  ASSERT_EQ(RmcProxyImpl::RMC_LIBRARY_SLOT_TYPE_MANUAL, rmc.getLibrarySlotType("manual"));
  ASSERT_EQ(RmcProxyImpl::RMC_LIBRARY_SLOT_TYPE_SCSI, rmc.getLibrarySlotType("smc@rmc_host,1"));
  ASSERT_EQ(RmcProxyImpl::RMC_LIBRARY_SLOT_TYPE_UNKNOWN, rmc.getLibrarySlotType("nonsense"));
}

} // namespace unitTests
