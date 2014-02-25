/******************************************************************************
 *             castor/tape/rmc/AcsTest.hpp
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

#include "castor/tape/rmc/MockAcs.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_rmc_AcsTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class castor_tape_rmc_AcsTest

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithValidDriveId) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:113:114";
  DRIVEID driveId;
  MockAcs acs;
  ASSERT_NO_THROW(driveId = acs.str2DriveId(str));
  ASSERT_EQ((ACS)111, driveId.panel_id.lsm_id.acs);
  ASSERT_EQ((LSM)112, driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ((PANEL)113, driveId.panel_id.panel);
  ASSERT_EQ((DRIVE)114, driveId.drive);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithEmptyString) {
  using namespace castor::tape::rmc;
  const std::string str = "";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveWithPrecedingZeros) {
  using namespace castor::tape::rmc;
  const std::string str = "011:012:013:014";
  DRIVEID driveId;
  MockAcs acs;
  ASSERT_NO_THROW(driveId = acs.str2DriveId(str));
  ASSERT_EQ((ACS)11, driveId.panel_id.lsm_id.acs);
  ASSERT_EQ((LSM)12, driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ((PANEL)13, driveId.panel_id.panel);
  ASSERT_EQ((DRIVE)14, driveId.drive);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithTooManyColons) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:113:114:115";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, driveId2StrWithArbitrary3DigitValues) {
  using namespace castor::tape::rmc;
  DRIVEID driveId;
  driveId.panel_id.lsm_id.acs = (ACS)111;
  driveId.panel_id.lsm_id.lsm = (LSM)112;
  driveId.panel_id.panel = (PANEL)113;
  driveId.drive = (DRIVE)114;
  MockAcs acs;
  ASSERT_EQ(std::string("111:112:113:114"), acs.driveId2Str(driveId));
}

TEST_F(castor_tape_rmc_AcsTest, driveId2StrWithArbitrary1DigitValues) {
  using namespace castor::tape::rmc;
  DRIVEID driveId;
  driveId.panel_id.lsm_id.acs = (ACS)1;
  driveId.panel_id.lsm_id.lsm = (LSM)2;
  driveId.panel_id.panel = (PANEL)3;
  driveId.drive = (DRIVE)4;
  MockAcs acs;
  ASSERT_EQ(std::string("001:002:003:004"), acs.driveId2Str(driveId));
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithTooLongAcs) {
  using namespace castor::tape::rmc;
  const std::string str = "1111:112:113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithTooLongLsm) {
  using namespace castor::tape::rmc;
  const std::string str = "111:1122:113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithTooLongPanel) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:1133:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithTooLongTransport) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:113:1144";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithNonNumericAcs) {
  using namespace castor::tape::rmc;
  const std::string str = "acs:112:113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithNonNumericLsm) {
  using namespace castor::tape::rmc;
  const std::string str = "111:lsm:113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithNonNumericPanel) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:pan:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithNonNumericTransport) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:113:tra";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithEmptyAcs) {
  using namespace castor::tape::rmc;
  const std::string str = ":112:113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithEmptyLsm) {
  using namespace castor::tape::rmc;
  const std::string str = "111::113:114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithEmptyPanel) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112::114";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2DriveIdWithEmptyTransport) {
  using namespace castor::tape::rmc;
  const std::string str = "111:112:113:";
  MockAcs acs;
  ASSERT_THROW(acs.str2DriveId(str), castor::exception::InvalidArgument);
}

TEST_F(castor_tape_rmc_AcsTest, str2VolidWithEmptyString) {
  using namespace castor::tape::rmc;
  const std::string str = "";
  VOLID volId;
  MockAcs acs;
  ASSERT_NO_THROW(volId = acs.str2Volid(str));
  ASSERT_EQ('\0', volId.external_label[0]);
}

TEST_F(castor_tape_rmc_AcsTest, str2VolidWith6CharacterString) {
  using namespace castor::tape::rmc;
  const std::string str = "123456";
  VOLID volId;
  MockAcs acs;
  ASSERT_NO_THROW(volId = acs.str2Volid(str));
  ASSERT_EQ(str, std::string(volId.external_label));
}

TEST_F(castor_tape_rmc_AcsTest, str2VolidWith7CharacterString) {
  using namespace castor::tape::rmc;
  const std::string str = "1234567";
  MockAcs acs;
  ASSERT_THROW(acs.str2Volid(str), castor::exception::InvalidArgument);
}

} // namespace unitTests
