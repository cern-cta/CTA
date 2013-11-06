/******************************************************************************
 *             test/unittest/castor/tape/dismountacs/AcsTest.hpp
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

#include "test/unittest/castor/tape/rmc/MockAcs.hpp"

#include <cppunit/extensions/HelperMacros.h>

namespace castor {
namespace tape {
namespace rmc {

class AcsTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testStr2DriveIdWithValidDriveId() {
    const std::string str = "111:112:113:114";
    DRIVEID driveId;
    MockAcs acs;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Calling str2DriveId with valid drive ID",
      driveId = acs.str2DriveId(str));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      (ACS)111, driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      (LSM)112, driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      (PANEL)113, driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      (DRIVE)114, driveId.drive);
  }

  void testStr2DriveIdWithEmptyString() {
    const std::string str = "";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with empty string",
      acs.str2DriveId(str),
      castor::exception::InvalidArgument);
  }

  void testStr2DriveWithPrecedingZeros() {
    const std::string str = "011:012:013:014";
    DRIVEID driveId;
    MockAcs acs;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Calling str2DriveId with preceding zeros",
      driveId = acs.str2DriveId(str));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing ACS number",
      (ACS)11, driveId.panel_id.lsm_id.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing LSM number",
      (LSM)12, driveId.panel_id.lsm_id.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing panel number",
      (PANEL)13, driveId.panel_id.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing drive number",
      (DRIVE)14, driveId.drive);
  }

  void testStr2DriveIdWithTooManyColons() {
    const std::string str = "111:112:113:114:115";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with too many colons",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testDriveId2StrWithArbitrary3DigitValues() {
    DRIVEID driveId;
    driveId.panel_id.lsm_id.acs = (ACS)111;
    driveId.panel_id.lsm_id.lsm = (LSM)112;
    driveId.panel_id.panel = (PANEL)113;
    driveId.drive = (DRIVE)114;
    MockAcs acs;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing str()",
      std::string("111:112:113:114"), acs.driveId2Str(driveId));
  }

  void testDriveId2StrWithArbitrary1DigitValues() {
    DRIVEID driveId;
    driveId.panel_id.lsm_id.acs = (ACS)1;
    driveId.panel_id.lsm_id.lsm = (LSM)2;
    driveId.panel_id.panel = (PANEL)3;
    driveId.drive = (DRIVE)4;
    MockAcs acs;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Testing str()",
      std::string("001:002:003:004"), acs.driveId2Str(driveId));
  }

  void testStr2DriveIdWithTooLongAcs() {
    const std::string str = "1111:112:113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with too long ACS",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithTooLongLsm() {
    const std::string str = "111:1122:113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with too long LSM",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithTooLongPanel() {
    const std::string str = "111:112:1133:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with too long panel",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithTooLongTransport() {
    const std::string str = "111:112:113:1144";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with too long drive",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithNonNumericAcs() {
    const std::string str = "acs:112:113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with non-numeric ACS",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithNonNumericLsm() {
    const std::string str = "111:lsm:113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with non-numeric LSM",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithNonNumericPanel() {
    const std::string str = "111:112:pan:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with non-numeric panel",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithNonNumericTransport() {
    const std::string str = "111:112:113:tra";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Calling str2DriveId with non-numeric drive",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithEmptyAcs() {
    const std::string str = ":112:113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with empty ACS",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithEmptyLsm() {
    const std::string str = "111::113:114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with empty LSM",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithEmptyPanel() {
    const std::string str = "111:112::114";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with empty panel",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2DriveIdWithEmptyTransport() {
    const std::string str = "111:112:113:";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2DriveId with empty drive",
      acs.str2DriveId(str), castor::exception::InvalidArgument);
  }

  void testStr2VolidWithEmptyString() {
    const std::string str = "";
    VOLID volId;
    MockAcs acs;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Calling str2Volid with empty string",
      volId = acs.str2Volid(str));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking external_label is an empty string",
      '\0', volId.external_label[0]);
  }

  void testStr2VolidWith6CharacterString() {
    const std::string str = "123456";
    VOLID volId;
    MockAcs acs;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Calling str2Volid with 6 character string",
      volId = acs.str2Volid(str));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking external_label",
      str, std::string(volId.external_label));
  }

  void testStr2VolidWith7CharacterString() {
    const std::string str = "1234567";
    MockAcs acs;
    CPPUNIT_ASSERT_THROW_MESSAGE("Calling str2Volid with 7 character string",
      acs.str2Volid(str), castor::exception::InvalidArgument);
  }

  CPPUNIT_TEST_SUITE(AcsTest);
  CPPUNIT_TEST(testStr2DriveIdWithValidDriveId);
  CPPUNIT_TEST(testStr2DriveIdWithEmptyString);
  CPPUNIT_TEST(testStr2DriveWithPrecedingZeros);
  CPPUNIT_TEST(testStr2DriveIdWithTooManyColons);
  CPPUNIT_TEST(testStr2DriveIdWithTooLongAcs);
  CPPUNIT_TEST(testStr2DriveIdWithTooLongLsm);
  CPPUNIT_TEST(testStr2DriveIdWithTooLongPanel);
  CPPUNIT_TEST(testStr2DriveIdWithTooLongTransport);
  CPPUNIT_TEST(testStr2DriveIdWithNonNumericAcs);
  CPPUNIT_TEST(testStr2DriveIdWithNonNumericLsm);
  CPPUNIT_TEST(testStr2DriveIdWithNonNumericPanel);
  CPPUNIT_TEST(testStr2DriveIdWithNonNumericTransport);
  CPPUNIT_TEST(testStr2DriveIdWithEmptyAcs);
  CPPUNIT_TEST(testStr2DriveIdWithEmptyLsm);
  CPPUNIT_TEST(testStr2DriveIdWithEmptyPanel);
  CPPUNIT_TEST(testStr2DriveIdWithEmptyTransport);
  CPPUNIT_TEST(testDriveId2StrWithArbitrary3DigitValues);
  CPPUNIT_TEST(testDriveId2StrWithArbitrary1DigitValues);
  CPPUNIT_TEST(testStr2VolidWith6CharacterString);
  CPPUNIT_TEST(testStr2VolidWith7CharacterString);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(AcsTest);

} // namespace rmc
} // namespace tape
} // namespace castor
