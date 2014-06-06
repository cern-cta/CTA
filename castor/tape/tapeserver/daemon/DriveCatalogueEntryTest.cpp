/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogueEntryTest.cpp
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

#include "castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_DriveCatalogueEntryTest :
  public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueEntryTest, drvState2Str) {
  using namespace castor::tape::tapeserver::daemon;

  ASSERT_EQ(std::string("INIT"),
    DriveCatalogueEntry::drvState2Str(DriveCatalogueEntry::DRIVE_STATE_INIT));
  ASSERT_EQ(std::string("DOWN"),
    DriveCatalogueEntry::drvState2Str(DriveCatalogueEntry::DRIVE_STATE_DOWN));
  ASSERT_EQ(std::string("UP"),
    DriveCatalogueEntry::drvState2Str(DriveCatalogueEntry::DRIVE_STATE_UP));
  ASSERT_EQ(std::string("WAITFORKTRANSFER"), DriveCatalogueEntry::drvState2Str(
    DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER));
  ASSERT_EQ(std::string("RUNNING"), DriveCatalogueEntry::drvState2Str(
    DriveCatalogueEntry::DRIVE_STATE_RUNNING));
  ASSERT_EQ(std::string("WAITDOWN"), DriveCatalogueEntry::drvState2Str(
    DriveCatalogueEntry::DRIVE_STATE_WAITDOWN));
}

} // namespace unitTests
