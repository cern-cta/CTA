/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/PendingMigrationsStoreTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP 1

#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "h/Cuuid.h"
#include "h/rtcpd_constants.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdlib.h>

class PendingMigrationsStoreTest: public CppUnit::TestFixture {

public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testGoodDayAddMarkGetAndRemove() {
    castor::tape::tapebridge::PendingMigrationsStore store;

    const uint32_t maxFseq = 1024;

    for(uint32_t i = 1; i <= maxFseq; i++) {
      castor::tape::tapegateway::FileToMigrate fileToMigrate;
      fileToMigrate.setFseq(i);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.add",
        store.add(fileToMigrate));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after add",
        i,
      store.getNbPendingMigrations());
    }

    for(uint32_t i = 1; i <= maxFseq; i++) {
      castor::tape::tapegateway::FileMigratedNotification notfication;
      notfication.setFseq(i);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.markAsWrittenWithoutFlush",
        store.markAsWrittenWithoutFlush(notfication));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after markAsWrittenWithoutFlush",
        maxFseq,
        store.getNbPendingMigrations());
    }

    for(uint32_t i = 1; i <= maxFseq; i++) {
      std::list<castor::tape::tapegateway::FileMigratedNotification>
        notfications;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.getAndRemoveFilesWrittenWithoutFlush",
        notfications = store.getAndRemoveFilesWrittenWithoutFlush(i));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "size of result of store.getAndRemoveFilesWrittenWithoutFlush",
        (std::list<castor::tape::tapegateway::FileMigratedNotification>::
        size_type)1,
        notfications.size());

      int32_t k = i;
      for(std::list<castor::tape::tapegateway::FileMigratedNotification>::
        const_iterator itor = notfications.begin(); itor != notfications.end();
        itor++) {

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "fseq of result of store.getAndRemoveFilesWrittenWithoutFlush",
          k,
          itor->fseq());

        k++;
      }

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after"
        " getAndRemoveFilesWrittenWithoutFlush",
        maxFseq - i,
        store.getNbPendingMigrations());
    }
  }

  CPPUNIT_TEST_SUITE(PendingMigrationsStoreTest);
  CPPUNIT_TEST(testGoodDayAddMarkGetAndRemove);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(PendingMigrationsStoreTest);

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
