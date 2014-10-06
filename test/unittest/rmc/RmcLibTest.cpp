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

#include "h/Castor_limits.h"
#include "h/rmc_find_char.h"
#include "h/rmc_get_acs_drive_id.h"
#include "h/rmc_get_rmc_host_of_drive.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>

class RmcLibTest: public CppUnit::TestFixture {
private:

  const char *const m_acsDrive;
  const char *const m_smcDrive;

public:

  RmcLibTest():
    m_acsDrive("acs1,23,456,7890"),
    m_smcDrive("smc@rmc_host_for_smc,123") {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testRmc_find_char_with_comma_at_9() {
    const char *const strWithComma = "012345678,012345,78";
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Find comma at index 9",
      9, rmc_find_char(strWithComma, ','));
  }

  void testRmc_find_char_with_no_comma() {
    const char *const strWithoutComma = "0123456789012345678";
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Find comma that does not exist",
      -1, rmc_find_char(strWithoutComma, ','));
  }

  void testRmc_get_rmc_host_of_drive_with_acs_drive() {
    char rmcHost[CA_MAXHOSTNAMELEN+1];
    memset(rmcHost, '0', sizeof(rmcHost));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Good day get rmc host",
      0, rmc_get_rmc_host_of_drive(m_acsDrive, rmcHost, sizeof(rmcHost)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Test extracted rmc host has the right value",
      std::string("rmc_host_for_acs"), std::string(rmcHost));
  }

  void testRmc_get_rmc_host_of_drive_with_smc_drive() {
    char rmcHost[CA_MAXHOSTNAMELEN+1];
    memset(rmcHost, '0', sizeof(rmcHost));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Good day get rmc host",
      0, rmc_get_rmc_host_of_drive(m_smcDrive, rmcHost, sizeof(rmcHost)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Test extracted rmc host has the right value",
      std::string("rmc_host_for_smc"), std::string(rmcHost));
  }

  void testRmc_getacs_drive_id() {
    rmc_acs_drive_id driveId = {0, 0, 0, 0};
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Good day get acs drive id",
      0, rmc_get_acs_drive_id(m_acsDrive, &driveId));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking ACS number",
      1, driveId.acs);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking LSM number",
      23, driveId.lsm);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking panel number",
      456, driveId.panel);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Checking transport number",
      7890, driveId.transport);
  }

  CPPUNIT_TEST_SUITE(RmcLibTest);
  CPPUNIT_TEST(testRmc_find_char_with_comma_at_9);
  CPPUNIT_TEST(testRmc_find_char_with_no_comma);
  CPPUNIT_TEST(testRmc_get_rmc_host_of_drive_with_acs_drive);
  CPPUNIT_TEST(testRmc_get_rmc_host_of_drive_with_smc_drive);
  CPPUNIT_TEST(testRmc_getacs_drive_id);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(RmcLibTest);
