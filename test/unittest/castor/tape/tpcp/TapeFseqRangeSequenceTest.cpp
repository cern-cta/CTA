/******************************************************************************
 *    test/unittest/castor/tape/tpcp/TapeFseqRangeSequenceTest.hpp
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

#include "castor/tape/tpcp/TapeFseqRangeSequence.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>

namespace castor {
namespace tape   {
namespace tpcp   {

class TapeFseqRangeSequenceTest: public CppUnit::TestFixture {
public:
  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testEmptyTapeFseqRangeSequence() {
    TapeFseqRangeSequence emptySeq;

    CPPUNIT_ASSERT_MESSAGE(
      "Checking emptySeq.hasMore() is false",
      !emptySeq.hasMore());
  }

  void testTapeFseqRangeSequenceFrom21To29() {
    TapeFseqRange range21To29(21, 29);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range21To29.lower() is 21",
      (uint32_t)21,
      range21To29.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range21To29.upper() is 29",
      (uint32_t)29,
      range21To29.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range21To29.size() is 9",
      (uint32_t)9,
      range21To29.size());

    TapeFseqRangeSequence seq21To29(range21To29);

    uint32_t expected = 0;
    uint32_t actual   = 0;

    for(expected=21 ;expected<=29; expected++) {
      CPPUNIT_ASSERT_MESSAGE(
        "Checking seq21To29.hasMore() is true",
        seq21To29.hasMore());

      actual = seq21To29.next();

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking seq21To29.next()",
        expected,
        actual);
    }
  }

  CPPUNIT_TEST_SUITE(TapeFseqRangeSequenceTest);
  CPPUNIT_TEST(testEmptyTapeFseqRangeSequence);
  CPPUNIT_TEST(testTapeFseqRangeSequenceFrom21To29);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeFseqRangeSequenceTest);

} // namespace tpcp
} // namespace tape
} // namespace castor
