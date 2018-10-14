/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mediachanger/acs/AcsCmdLine.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_mediachanger_acs_AcsCmdLineTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

/**
 * Tests good day senario with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, goodDayStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3:4");
  const auto driveId = AcsCmdLine::str2DriveId(str);

  ASSERT_EQ(1, driveId.panel_id.lsm_id.acs);
  ASSERT_EQ(2, driveId.panel_id.lsm_id.lsm);
  ASSERT_EQ(3, driveId.panel_id.panel);
  ASSERT_EQ(4, driveId.drive);
}

/**
 * Tests too many components with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooManyComponentsStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3:4:5");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests too few components with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooFewComponentsStr2DriveIdgoodDayStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests tool long component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooLongAcsComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1111:2:3:4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests tool long component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooLongLsmComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2222:3:4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests tool long component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooLongPanComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3333:4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests tool long component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, tooLongDrvComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3:4444");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests empty component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, emptyAcsComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str(":2:3:4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests empty component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, emptyLsmComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1::3:4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests empty component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, emptyPanComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2::4");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

/**
 * Tests empty component with Acs::strToDrveId.
 */
TEST_F(cta_mediachanger_acs_AcsCmdLineTest, emptyDrvComponentStr2DriveId) {
  using namespace cta::mediachanger::acs;
  const std::string str("1:2:3:");
  ASSERT_THROW(AcsCmdLine::str2DriveId(str), AcsCmdLine::InvalidDriveId);
}

} // namespace unitTests
