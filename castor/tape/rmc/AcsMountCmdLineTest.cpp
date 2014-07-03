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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/rmc/AcsMountCmdLine.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_rmc_AcsMountCmdLineTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_rmc_AcsMountCmdLineTest, constructor) {
  const castor::tape::rmc::AcsMountCmdLine cmdLine;
  ASSERT_EQ(false, cmdLine.debug);
  ASSERT_EQ(false, cmdLine.help);
  ASSERT_EQ((BOOLEAN)FALSE, cmdLine.readOnly);
  ASSERT_EQ(0, cmdLine.timeout);
  ASSERT_EQ(0, cmdLine.queryInterval);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(0, (int)cmdLine.driveId.panel_id.panel);
  ASSERT_EQ(0, (int)cmdLine.driveId.drive);
  ASSERT_EQ('\0', cmdLine.volId.external_label[0]);
}

} // namespace unitTests
