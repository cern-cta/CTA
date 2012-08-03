/******************************************************************************
 *    test/unittest/tape/InitlabelTest.hpp
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

#include "h/Ctape.h"
#include "h/serrno.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <string>

class InitlabelTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  /**
   * Simple test that calls all the function of initlabel.c to prove they are
   * working sensibly.
   */
  void testInitlabelroutinesWith1() {
    char path[CA_MAXPATHLEN+1];
    char devtype[CA_MAXDVTLEN+1];
    const int den = D5000GC;
    const int lblcode = AUL;
    const int flags = 0;
    const int fseq = 4;
    char vol1[81] = "VOL1 label";
    char hdr1[81] = "HDR1 label";
    char hdr2[81] = "HDR2 label";
    char uhl1[81] = "UHL1 label";;
    struct devlblinfo *dlip = NULL;

    memset(path, '\0', sizeof(path));
    strncpy(path, "path", sizeof(path) - 1);
    memset(devtype, '\0', sizeof(devtype));
    strncpy(devtype, "devType", sizeof(devtype) - 1);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling initlabelroutines()",
      0,
      initlabelroutines(1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setdevinfo()",
      0,
      setdevinfo(path, devtype, den, lblcode));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setlabelinfo()",
      0,
      setlabelinfo(path, flags, fseq, vol1, hdr1, hdr2, uhl1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling getlabelinfo()",
      0,
      getlabelinfo(path, &dlip));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking path of getlabelinfo result",
      std::string(path),
      std::string(dlip->path));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking devtype of getlabelinfo result",
      std::string(devtype),
      std::string(dlip->devtype));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking lblcode of getlabelinfo result",
      lblcode,
      dlip->lblcode);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking flags of getlabelinfo result",
      flags,
      dlip->flags);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking fseq of getlabelinfo result",
      fseq,
      dlip->fseq);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking vol1 of getlabelinfo result",
      std::string(vol1),
      std::string(dlip->vol1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking hdr1 of getlabelinfo result",
      std::string(hdr1),
      std::string(dlip->hdr1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking hdr2 of getlabelinfo result",
      std::string(hdr2),
      std::string(dlip->hdr2));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking uhl1 of getlabelinfo result",
      std::string(uhl1),
      std::string(dlip->uhl1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling rmlabelinfo()",
      0,
      rmlabelinfo(path, flags));
  }

  /**
   * Support for a multiple drive resource lock is being removed as it buggy
   * and never used.  The new implementation of initlabelroutines should
   * therefore not accept a value other than 1 and in all other cases return -1
   * and set serrno to EINVAL.
   */
  void testInitlabelroutinesWith2() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling initlabelroutines()",
      -1,
      initlabelroutines(2));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking initlabelroutines sets serrno occordingly",
      EINVAL,
      serrno);
  }

  CPPUNIT_TEST_SUITE(InitlabelTest);
  CPPUNIT_TEST(testInitlabelroutinesWith1);
  CPPUNIT_TEST(testInitlabelroutinesWith2);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(InitlabelTest);

#endif // TEST_UNITTEST_TAPE_INITLABELTEST
