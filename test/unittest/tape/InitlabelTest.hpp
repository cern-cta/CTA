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

#include "castor/tape/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/serrno.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <string>

class InitlabelTest: public CppUnit::TestFixture {
private:
  char m_path_1[CA_MAXPATHLEN+1]; /* CA_MAXPATHLEN = 1023 */
  char m_devtype_1[CA_MAXDVTLEN+1]; /* CA_MAXDVTLEN = 8 */
  const int m_den_1;
  const int m_lblcode_1;
  const int m_flags_1;
  const int m_fseq_1;
  char m_vol1_1[81];
  char m_hdr1_1[81];
  char m_hdr2_1[81];
  char m_uhl1_1[81];

  char m_path_2[CA_MAXPATHLEN+1]; /* CA_MAXPATHLEN = 1023 */
  char m_devtype_2[CA_MAXDVTLEN+1]; /* CA_MAXDVTLEN = 8 */
  const int m_den_2;
  const int m_lblcode_2;
  const int m_flags_2;
  const int m_fseq_2;
  char m_vol1_2[81];
  char m_hdr1_2[81];
  char m_hdr2_2[81];
  char m_uhl1_2[81];

public:

  InitlabelTest():
    m_den_1(D5000GC),
    m_lblcode_1(AUL),
    m_flags_1(0),
    m_fseq_1(4),
    m_den_2(D5000GC),
    m_lblcode_2(AUL),
    m_flags_2(0),
    m_fseq_2(4) {
    castor::tape::utils::copyString(m_path_1, "path1");
    castor::tape::utils::copyString(m_devtype_1, "devtype1");
    castor::tape::utils::copyString(m_vol1_1, "VOL1 label 1");
    castor::tape::utils::copyString(m_hdr1_1, "HDR1 label 1");
    castor::tape::utils::copyString(m_hdr2_1, "HDR2 label 1");
    castor::tape::utils::copyString(m_uhl1_1, "UHL1 label 1");

    castor::tape::utils::copyString(m_path_2, "path2");
    castor::tape::utils::copyString(m_devtype_2, "devtype2");
    castor::tape::utils::copyString(m_vol1_2, "VOL1 label 2");
    castor::tape::utils::copyString(m_hdr1_2, "HDR1 label 2");
    castor::tape::utils::copyString(m_hdr2_2, "HDR2 label 2");
    castor::tape::utils::copyString(m_uhl1_2, "UHL1 label 2");
  }

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    const int rmlabelinfoFlags = 0;
    rmlabelinfo(m_path_1, rmlabelinfoFlags);
  }

  /**
   * Simple test that calls all of the functions of initlabel.c to prove they
   * are working sensibly.
   */
  void testInitlabelroutines() {
    struct devlblinfo *dlip = NULL;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setdevinfo()",
      0,
      setdevinfo(m_path_1, m_devtype_1, m_den_1, m_lblcode_1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setlabelinfo()",
      0,
      setlabelinfo(m_path_1, m_flags_1, m_fseq_1, m_vol1_1, m_hdr1_1, m_hdr2_1,
        m_uhl1_1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling getlabelinfo()",
      0,
      getlabelinfo(m_path_1, &dlip));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking path of getlabelinfo result",
      std::string(m_path_1),
      std::string(dlip->path));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking devtype of getlabelinfo result",
      std::string(m_devtype_1),
      std::string(dlip->devtype));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking lblcode of getlabelinfo result",
      m_lblcode_1,
      dlip->lblcode);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking flags of getlabelinfo result",
      m_flags_1,
      dlip->flags);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking fseq of getlabelinfo result",
      m_fseq_1,
      dlip->fseq);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking vol1 of getlabelinfo result",
      std::string(m_vol1_1),
      std::string(dlip->vol1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking hdr1 of getlabelinfo result",
      std::string(m_hdr1_1),
      std::string(dlip->hdr1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking hdr2 of getlabelinfo result",
      std::string(m_hdr2_1),
      std::string(dlip->hdr2));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking uhl1 of getlabelinfo result",
      std::string(m_uhl1_1),
      std::string(dlip->uhl1));
  }

  /**
   * Calling setdevinfo() a second time with a differnet path and without
   * calling rmlabelinfo() should return -1 and set serrno to ENOMEM.
   */
  void testSetdevinfoTwiceWithDifferentPaths() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setdevinfo() with m_path_1",
      0,
      setdevinfo(m_path_1, m_devtype_1, m_den_1, m_lblcode_1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setdevinfo() with m_path_2",
      -1,
      setdevinfo(m_path_2, m_devtype_2, m_den_2, m_lblcode_2));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking setlabelinfo sets serrno occordingly",
      ENOMEM,
      serrno);
  }

  /**
   * setlabelinfo() should return -1 and set serrno to ETNOLBLINFO if it is
   * called before setdevinfo().
   */
  void testSetLabelinfoWithoutFirstCallingSetdevinfo() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling setlabelinfo()",
      -1,
      setlabelinfo(m_path_1, m_flags_1, m_fseq_1, m_vol1_1, m_hdr1_1, m_hdr2_1,
        m_uhl1_1));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking setlabelinfo sets serrno occordingly",
      ETNOLBLINFO,
      serrno);
  }

  /**
   * Callinging getlabelinfo() without calling setdevinfo() first should return -1
   * and set serrno to ETNOLBLINFO.
   */
  void testGetNonExistentLabelInfo() {
    char path[CA_MAXPATHLEN+1];
    struct devlblinfo *dlip = NULL;

    memset(path, '\0', sizeof(path));
    strncpy(path, "non_existent_path", sizeof(path) - 1);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Calling getlabelinfo()",
      -1,
      getlabelinfo(path, &dlip));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking initlabelroutines sets serrno occordingly",
      ETNOLBLINFO,
      serrno);
  }

  CPPUNIT_TEST_SUITE(InitlabelTest);
  CPPUNIT_TEST(testInitlabelroutines);
  CPPUNIT_TEST(testSetdevinfoTwiceWithDifferentPaths);
  CPPUNIT_TEST(testSetLabelinfoWithoutFirstCallingSetdevinfo);
  CPPUNIT_TEST(testGetNonExistentLabelInfo);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(InitlabelTest);

#endif // TEST_UNITTEST_TAPE_INITLABELTEST
