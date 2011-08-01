/******************************************************************************
 *                test/castor/tape/utils/IndexedContainerTest.hpp
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

#ifndef TEST_CASTOR_TAPE_UTILS_INDEXEDCONTAINERTEST_HPP
#define TEST_CASTOR_TAPE_UTILS_INDEXEDCONTAINERTEST_HPP 1

#include "castor/tape/utils/IndexedContainer.hpp"
#include "test/castor/tape/utils/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <list>
#include <stdlib.h>
#include <string>
#include <vector>

class IndexedContainerTest: public CppUnit::TestFixture {
public:
  castor::tape::utils::IndexedContainer<const void *> *m_container;

  const void *m_ptr1;
  const void *m_ptr2;
  const void *m_ptr3;
  const void *m_ptr4;
  int m_ptr1Index;
  int m_ptr2Index;
  int m_ptr3Index;

  IndexedContainerTest():
    m_container(NULL),
    m_ptr1((const void *)0x12),
    m_ptr2((const void *)0x34),
    m_ptr3((const void *)0x56),
    m_ptr4((const void *)0x78),
    m_ptr1Index(-1),
    m_ptr2Index(-1),
    m_ptr3Index(-1) {
  }

  void setUp() {
    m_container = new castor::tape::utils::IndexedContainer<const void *>(3);
    m_ptr1Index = m_container->insert(m_ptr1);
    m_ptr2Index = m_container->insert(m_ptr2);
    m_ptr3Index = m_container->insert(m_ptr3);
  }

  void tearDown() {
    delete(m_container);
    m_container = NULL;
  }

  void testTooManyPointers() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Adding one too many pointers",
      m_container->insert(m_ptr4), castor::exception::OutOfMemory);
  }

  void testRemoveUsingNegativeIndex() {
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Removing an element using the invalid index of -1",
      m_container->remove(-1), castor::exception::InvalidArgument);
  }

  void testRemoveUsingTooLargeIndex() {
    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Removing an element using the invalid index of 3",
      m_container->remove(3), castor::exception::InvalidArgument);
  }

  void testGoodDayRemove() {
    const void *removedPtr2 = m_container->remove(1);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Removing element with index 1", m_ptr2,
      removedPtr2);
  }

  void testRemoveFreeElement() {
    const void *removedPtr2 = m_container->remove(1);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Removing element with index 1", m_ptr2,
      removedPtr2);

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Illegally removing the free element with index 1",
      m_container->remove(1), castor::exception::NoEntry);
  }

  CPPUNIT_TEST_SUITE(IndexedContainerTest);
  CPPUNIT_TEST(testTooManyPointers);
  CPPUNIT_TEST(testRemoveUsingNegativeIndex);
  CPPUNIT_TEST(testRemoveUsingTooLargeIndex);
  CPPUNIT_TEST(testGoodDayRemove);
  CPPUNIT_TEST(testRemoveFreeElement);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexedContainerTest);

#endif // TEST_CASTOR_TAPE_UTILS_INDEXEDCONTAINERTEST_HPP
