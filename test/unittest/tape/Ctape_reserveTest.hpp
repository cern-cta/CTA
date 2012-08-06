/******************************************************************************
 *    test/unittest/tape/Ctape_reserveTest.hpp
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

#ifndef TEST_UNITTEST_TAPE_CTAPERESERVETESTTEST_HPP
#define TEST_UNITTEST_TAPE_CTAPERESERVETESTTEST_HPP 1

#include "h/Ctape_api.h"
#include "h/serrno.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <string>
#include <string.h>

class Ctape_reserveTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  /**
   * Support for a multiple drive resource lock is being removed as it buggy
   * and never used.  The new implementation of Ctape_reserve should
   * therefore not accept a count parameter value other than 1 and in all other
   * cases return -1 and set serrno to EINVAL.
   */
  void testCtape_reserveWithCountOf2() {
    const int count = 2;
    struct dgn_rsv dgn_reservations[2];

    memset(dgn_reservations, '\0', sizeof(dgn_reservations));

    strncpy(dgn_reservations[0].name, "DGN1",
      sizeof(dgn_reservations[0].name - 1));
    dgn_reservations[0].num = 1;
    strncpy(dgn_reservations[1].name, "DGN2",
      sizeof(dgn_reservations[1].name - 1));
    dgn_reservations[1].num = 1;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling Ctape_reserve()",
      -1,
      Ctape_reserve(count, dgn_reservations));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking serrno is set occordingly",
      ETMLTDRVRSV,
      serrno);
  }

  /**
   * Support for a multiple drive resource lock is being removed as it buggy
   * and never used.  The new implementation of Ctape_reserve should
   * therefore not accept a DGN reservation structure with a drive count other
   * than 1 and in all other cases return -1 and set serrno to EINVAL.
   */
  void testCtape_reserveWith1DgnReservationWithDriveCountOf2() {
    const int count = 1;
    struct dgn_rsv dgn_reservation;

    memset(&dgn_reservation, '\0', sizeof(dgn_reservation));
    strncpy(dgn_reservation.name, "DGN1", sizeof(dgn_reservation.name) - 1);
    dgn_reservation.num = 2;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling Ctape_reserve()",
      -1,
      Ctape_reserve(count, &dgn_reservation));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking serrno is set occordingly",
      ETMLTDRVRSV,
      serrno);
  }

  CPPUNIT_TEST_SUITE(Ctape_reserveTest);
  CPPUNIT_TEST(testCtape_reserveWithCountOf2);
  CPPUNIT_TEST(testCtape_reserveWith1DgnReservationWithDriveCountOf2);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(Ctape_reserveTest);

#endif // TEST_UNITTEST_TAPE_CTAPERESERVETESTTEST
