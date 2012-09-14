/******************************************************************************
 *    test/unittest/tape/CvtdenTest.hpp
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

#ifndef TEST_UNITTEST_TAPE_INITLABELTEST_HPP
#define TEST_UNITTEST_TAPE_INITLABELTEST_HPP 1

#include "castor/tape/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/serrno.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <string>

class CvtdenTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testCvtdenWith1000G() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1000G\")",
      D1000G,
      cvtden("1000G"));
  }

  void testCvtdenWith1000GC() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1000GC\")",
      D1000GC,
      cvtden("1000GC"));
  }

  void testCvtdenWith1500G() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1500G\")",
      D1500G,
      cvtden("1500G"));
  }

  void testCvtdenWith1500GC() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1500GC\")",
      D1500GC,
      cvtden("1500GC"));
  }

  void testCvtdenWith1600G() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1600G\")",
      D1600G,
      cvtden("1600G"));
  }

  void testCvtdenWith1600GC() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"1600GC\")",
      D1600GC,
      cvtden("1600GC"));
  }

  void testCvtdenWith4000G() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"4000G\")",
      D4000G,
      cvtden("4000G"));
  }

  void testCvtdenWith4000GC() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"4000GC\")",
      D4000GC,
      cvtden("4000GC"));
  }

  void testCvtdenWith5000G() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"5000G\")",
      D5000G,
      cvtden("5000G"));
  }

  void testCvtdenWith5000GC() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling cvtden(\"5000GC\")",
      D5000GC,
      cvtden("5000GC"));
  }

  CPPUNIT_TEST_SUITE(CvtdenTest);
  CPPUNIT_TEST(testCvtdenWith1000G);
  CPPUNIT_TEST(testCvtdenWith1000GC);
  CPPUNIT_TEST(testCvtdenWith1500G);
  CPPUNIT_TEST(testCvtdenWith1500GC);
  CPPUNIT_TEST(testCvtdenWith1600G);
  CPPUNIT_TEST(testCvtdenWith1600GC);
  CPPUNIT_TEST(testCvtdenWith4000G);
  CPPUNIT_TEST(testCvtdenWith4000GC);
  CPPUNIT_TEST(testCvtdenWith5000G);
  CPPUNIT_TEST(testCvtdenWith5000GC);
  CPPUNIT_TEST_SUITE_END();
};


CPPUNIT_TEST_SUITE_REGISTRATION(CvtdenTest);

#endif // TEST_UNITTEST_TAPE_INITLABELTEST
