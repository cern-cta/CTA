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

#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>

namespace castor {
namespace tape   {
namespace tpcp   {

class TapeFseqRangeListSequenceTest: public CppUnit::TestFixture {
public:
  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testEmptyTapeFseqRangeListSequence() {
    TapeFseqRangeListSequence emptyListSeq;

    CPPUNIT_ASSERT_MESSAGE(
      "Checking emptyListSeq.isFinite() is true",
      emptyListSeq.isFinite());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking emptyListSeq.totalSize() is 0",
      (uint32_t)0,
      emptyListSeq.totalSize());

    CPPUNIT_ASSERT_MESSAGE(
      "Checking emptyListSeq.hasMore() is false",    
      !emptyListSeq.hasMore());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking emptyListSeq.totalSize() is 0",
      (uint32_t)0,
      emptyListSeq.totalSize());
  }

  void testTapeFseqRangeListSequenceFrom7To9() {
    TapeFseqRange range7to9(7, 9);
    TapeFseqRangeList list7to9;
    list7to9.push_back(range7to9);
    TapeFseqRangeListSequence listSeq7to9(&list7to9);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range7to9.lower() is 7",
      (uint32_t)7,
      range7to9.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range7to9.upper() is 9",
      (uint32_t)9,
      range7to9.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking range7to9.size() is 3",
      (uint32_t)3,
      range7to9.size());

    CPPUNIT_ASSERT_MESSAGE(
      "Checking listSeq7to9.isFinite() is true",
      listSeq7to9.isFinite());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking listSeq7to9.totalSize() is 3",
      (uint32_t)3,
      listSeq7to9.totalSize());
  }

  void testTapeFseqRangeListSequenceFrom7ToInfinity() {
    TapeFseqRange range7toInfinity(7, 0);
    TapeFseqRangeList list7toInfinity;
    list7toInfinity.push_back(range7toInfinity);
    TapeFseqRangeListSequence listSeq7toInfinity(&list7toInfinity);

    CPPUNIT_ASSERT_MESSAGE(
      "Checking listSeq7toInfinity.isFinite() is false",
      !listSeq7toInfinity.isFinite());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking listSeq7toInfinity.totalSize() is 0",
      (uint32_t)0,
      listSeq7toInfinity.totalSize());
  }

  void testComplexTapeFseqRangeListSequence() {
    TapeFseqRange range0( 1, 3);
    TapeFseqRange range1( 4,10);
    TapeFseqRange range2(21,29);
    TapeFseqRange range3(30,30);

    TapeFseqRangeList list;

    list.push_back(range0);
    list.push_back(range1);
    list.push_back(range2);
    list.push_back(range3);

    TapeFseqRangeListSequence listSeq(&list);

    uint32_t expected = 0;
    uint32_t actual   = 0;

    for(expected=1 ;expected<=10; expected++) {
      CPPUNIT_ASSERT_MESSAGE(
        "Checking listSeq.hasMore() is true",
        listSeq.hasMore());

      actual = listSeq.next();

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking listSeq.next()",
        expected,
        actual);
    }

    for(expected=21 ;expected<=30; expected++) {
      CPPUNIT_ASSERT_MESSAGE(
        "Checking listSeq.hasMore() is true",
        listSeq.hasMore());

      actual = listSeq.next();

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking listSeq.next()",
        expected,
        actual);
    }
  }

  CPPUNIT_TEST_SUITE(TapeFseqRangeListSequenceTest);
  CPPUNIT_TEST(testEmptyTapeFseqRangeListSequence);
  CPPUNIT_TEST(testTapeFseqRangeListSequenceFrom7To9);
  CPPUNIT_TEST(testTapeFseqRangeListSequenceFrom7ToInfinity);
  CPPUNIT_TEST(testComplexTapeFseqRangeListSequence);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeFseqRangeListSequenceTest);

} // namespace tpcp
} // namespace tape
} // namespace castor
