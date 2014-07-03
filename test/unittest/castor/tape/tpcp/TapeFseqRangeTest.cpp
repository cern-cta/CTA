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

#include "castor/tape/tpcp/TapeFseqRange.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>

namespace castor {
namespace tape   {
namespace tpcp   {

class TapeFseqRangeTest: public CppUnit::TestFixture {
public:
  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testTapeFseqRange0To1() {
    std::auto_ptr<TapeFseqRange> range;
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Expecting InvalidArgument exception with range(0, 1)",
      range.reset(new TapeFseqRange(0, 1)),
      castor::exception::InvalidArgument);
  }

   void testTapeFseqRange2To1() {
    std::auto_ptr<TapeFseqRange> range;
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Expecting InvalidArgument exception with range(2, 1)",
      range.reset(new TapeFseqRange(2, 1)),
      castor::exception::InvalidArgument);
  }

  void testEmptyTapeFseqRange() {
    TapeFseqRange emptyRange;

    CPPUNIT_ASSERT_MESSAGE(
      "Checking emptyRange.isEmpty() is true",
      emptyRange.isEmpty());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking emptyRange.lower() is 0",
      (uint32_t)0,
      emptyRange.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking emptyRange.upper() is 0",    
      (uint32_t)0,
      emptyRange.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking emptyRange.size() is 0",
      (uint32_t)0,
      emptyRange.size());
  }

  void testTapeFseqRangeF5ToInifinity() {
    TapeFseqRange from5ToInifinity(5, 0);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from5ToInifinity.lower() is 5",
      (uint32_t)5,
      from5ToInifinity.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from5ToInifinity.upper() is 0",
      (uint32_t)0,
      from5ToInifinity.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from5ToInifinity.size() is 0",
      (uint32_t)0,
      from5ToInifinity.size());

    CPPUNIT_ASSERT_MESSAGE(
      "Checking from5ToInifinity.isEmpty() is false",
      !from5ToInifinity.isEmpty());
  }

  void testTapeFseqRangeFrom7To7() {
    TapeFseqRange from7To7(7, 7);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from7To7.lower() is 7",
      (uint32_t)7,
      from7To7.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from7To7.upper() is 7",
      (uint32_t)7,
      from7To7.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from7To7.size() is 1",
      (uint32_t)1,
      from7To7.size());
  }

  void testTapeFseqRangeFrom21To29() {
    TapeFseqRange from21To29(21, 29);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from21To29.lower() is 21",
      (uint32_t)21,
      from21To29.lower());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from21To29.upper() is 29",
      (uint32_t)29,
      from21To29.upper());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking from21To29.size() is 9",
      (uint32_t)9,
      from21To29.size());
  }

  CPPUNIT_TEST_SUITE(TapeFseqRangeTest);
  CPPUNIT_TEST(testTapeFseqRange0To1);
  CPPUNIT_TEST(testTapeFseqRange2To1);
  CPPUNIT_TEST(testEmptyTapeFseqRange);
  CPPUNIT_TEST(testTapeFseqRangeF5ToInifinity);
  CPPUNIT_TEST(testTapeFseqRangeFrom7To7);
  CPPUNIT_TEST(testTapeFseqRangeFrom21To29);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeFseqRangeTest);

} // namespace tpcp
} // namespace tape
} // namespace castor
