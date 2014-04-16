/******************************************************************************
 *         castor/tape/tapeserver/daemon/ScsiLibraryDriveNameTest.cpp
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

#include "castor/tape/tapeserver/daemon/ScsiLibraryDriveName.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_ScsiLibraryDriveNameTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_ScsiLibraryDriveNameTest, goodDayParsing) {
  using namespace castor::tape::tapeserver::daemon;
 
  ScsiLibraryDriveName drive;
  ASSERT_EQ(std::string(""), drive.rmcHostName);
  ASSERT_EQ((uint16_t)0, drive.drvOrd);

  const std::string str = "smc@rmc_host,4";
  ASSERT_NO_THROW(drive = ScsiLibraryDriveName(str));
  ASSERT_EQ(std::string("rmc_host"), drive.rmcHostName);
  ASSERT_EQ((uint16_t)4, drive.drvOrd);
}

TEST_F(castor_tape_tapeserver_daemon_ScsiLibraryDriveNameTest, badDayParsing) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string str = "nonsense";
  ASSERT_THROW(ScsiLibraryDriveName drive(str), castor::exception::Exception);
}

} // namespace unitTests
