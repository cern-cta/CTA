/******************************************************************************
 *                test/rtcpd/TapeFileWaitingForFlushListTest.hpp
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

#ifndef TEST_RTCPD_TAPEFILEWAITINGFORFLUSHLISTTEST_HPP
#define TEST_RTCPD_TAPEFILEWAITINGFORFLUSHLISTTEST_HPP 1

#include "h/rtcpd_TapeFileWaitingForFlushList.h"
#include "h/serrno.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <string.h>

class TapeFileWaitingForFlushListTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testInitTapeFileWaitingForFlushList() {
    int rc           = 0;
    int saved_serrno = 0;

    rc = rtcpd_initTapeFileWaitingForFlushList(NULL);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_initTapeFileWaitingForFlushList(NULL)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_initTapeFileWaitingForFlushList(NULL)",
       EINVAL,
       saved_serrno);

    rtcpd_TapeFileWaitingForFlushList_t list;
    list.head = (rtcpd_TapeFileWaitingForFlush_t *)1111;
    list.tail = (rtcpd_TapeFileWaitingForFlush_t *)2222;
    list.nbElements = 3333;

    rc = rtcpd_initTapeFileWaitingForFlushList(&list);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_initTapeFileWaitingForFlushList(&list)",
       0,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.head for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.head);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.tail for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.tail);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.nbElements for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (uint32_t)0,
      list.nbElements);
  }

  void testFreeTapeFileWaitingForFlushList() {
    int rc           = 0;
    int saved_serrno = 0;

    rc = rtcpd_freeTapeFileWaitingForFlushList(NULL);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(NULL)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_freeTapeFileWaitingForFlushList(NULL)",
       EINVAL,
       saved_serrno);
    rtcpd_TapeFileWaitingForFlushList_t list;

    memset(&list, '\0', sizeof(list));
    list.tail = (rtcpd_TapeFileWaitingForFlush_t *)2222;

    rc = rtcpd_freeTapeFileWaitingForFlushList(&list);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(h=NULL t!=NULL)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_freeTapeFileWaitingForFlushList(h=NULL t!=NULL)",
       EINVAL,
       saved_serrno);

    memset(&list, '\0', sizeof(list));
    list.head = (rtcpd_TapeFileWaitingForFlush_t *)3333;

    rc = rtcpd_freeTapeFileWaitingForFlushList(&list);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(h!=NULL t=NULL)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_freeTapeFileWaitingForFlushList(h!=NULL t=NULL)",
       EINVAL,
       saved_serrno);

    memset(&list, '\0', sizeof(list));
    list.nbElements = 4444;

    rc = rtcpd_freeTapeFileWaitingForFlushList(&list);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(h=t=NULL nb!=0)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_freeTapeFileWaitingForFlushList(h=t=NULL nb!=0)",
       EINVAL,
       saved_serrno);

    rc = rtcpd_initTapeFileWaitingForFlushList(&list);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_initToTapeFileWaitingForFlushList(&list)",
      0,
      rc);

    const uint32_t totalNbElements = 1000;
    for(uint32_t i=1; i<=totalNbElements; i++) {
      rc = rtcpd_appendToTapeFileWaitingForFlushList(&list, i);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "rc rtcpd_appendToTapeFileWaitingForFlushList(&list, i)",
        0,
        rc);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "nbElements rtcpd_appendToTapeFileWaitingForFlushList(&list, 1111)",
      totalNbElements,
      list.nbElements);

    rc = rtcpd_freeTapeFileWaitingForFlushList(&list);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(&list)",
      0,
      rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.head rtcpd_freeTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.head);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.tail rtcpd_freeTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.tail);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.nbElements rtcpd_freeTapeFileWaitingForFlushList(&list)",
      (uint32_t)0,
      list.nbElements);
  }

  void testAppendToTapeFileWaitingForFlushList() {
    int rc           = 0;
    int saved_serrno = 0;

    rc = rtcpd_appendToTapeFileWaitingForFlushList(NULL, 1111);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_appendToTapeFileWaitingForFlushList(NULL, 1111)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_appendToTapeFileWaitingForFlushList(NULL, 1111)",
       EINVAL,
       saved_serrno);

    rtcpd_TapeFileWaitingForFlushList_t list;

    memset(&list, '\0', sizeof(list));
    list.tail = (rtcpd_TapeFileWaitingForFlush_t *)2222;

    rc = rtcpd_appendToTapeFileWaitingForFlushList(&list, 1111);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_appendToTapeFileWaitingForFlushList(h=NULL t!=NULL, 1111)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_appendToTapeFileWaitingForFlushList(h=NULL t!=NULL, 1111)",
       EINVAL,
       saved_serrno);

    memset(&list, '\0', sizeof(list));
    list.head = (rtcpd_TapeFileWaitingForFlush_t *)3333;

    rc = rtcpd_appendToTapeFileWaitingForFlushList(&list, 1111);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_appendToTapeFileWaitingForFlushList(h!=NULL t=NULL, 1111)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_appendToTapeFileWaitingForFlushList(h!=NULL t=NULL, 1111)",
       EINVAL,
       saved_serrno);

    memset(&list, '\0', sizeof(list));
    list.nbElements = 4444;

    rc = rtcpd_appendToTapeFileWaitingForFlushList(&list, 1111);
    saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_appendToTapeFileWaitingForFlushList(h=t=NULL nb!=0, 1111)",
       -1,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "serrno rtcpd_appendToTapeFileWaitingForFlushList(h=t=NULL nb!=0, 1111)",
       EINVAL,
       saved_serrno);

    list.head       = (rtcpd_TapeFileWaitingForFlush_t *)1111;
    list.tail       = (rtcpd_TapeFileWaitingForFlush_t *)2222;
    list.nbElements = 3333;

    rc = rtcpd_initTapeFileWaitingForFlushList(&list);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_initTapeFileWaitingForFlushList(&list)",
       0,
       rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.head for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.head);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.tail for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (rtcpd_TapeFileWaitingForFlush_t *)NULL,
      list.tail);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "list.nbElements for rtcpd_initTapeFileWaitingForFlushList(&list)",
      (uint32_t)0,
      list.nbElements);

    uint32_t i = 0;

    for(i=1; i<1000; i++) {
      rc = rtcpd_appendToTapeFileWaitingForFlushList(&list, i);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "rc rtcpd_appendToTapeFileWaitingForFlushList(&list, i)",
        0,
        rc);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "nbElements rtcpd_appendToTapeFileWaitingForFlushList(&list, i)",
        i,
        list.nbElements);
    }

    i = 1;
    for(rtcpd_TapeFileWaitingForFlush_t *element = list.head; element != NULL;
      element = element->next) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "element->value == i",
        (int)i,
        element->value);
      i++;
    }

    rc = rtcpd_freeTapeFileWaitingForFlushList(&list);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "rc rtcpd_freeTapeFileWaitingForFlushList(&list)",
      0,
      rc);
  }

  CPPUNIT_TEST_SUITE(TapeFileWaitingForFlushListTest);
  CPPUNIT_TEST(testInitTapeFileWaitingForFlushList);
  CPPUNIT_TEST(testFreeTapeFileWaitingForFlushList);
  CPPUNIT_TEST(testAppendToTapeFileWaitingForFlushList);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeFileWaitingForFlushListTest);

#endif // TEST_RTCPD_TAPEFILEWAITINGFORFLUSHLISTTEST_HPP
