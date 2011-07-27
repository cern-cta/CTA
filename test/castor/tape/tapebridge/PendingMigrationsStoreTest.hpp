/******************************************************************************
 *                test/castor/tape/tapebridge/PendingMigrationsStoreTest.hpp
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

#ifndef TEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
#define TEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP 1

#include "test_exception.hpp"
#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "h/Cuuid.h"
#include "h/rtcpd_constants.h"

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

    castor::tape::tapegateway::FileToMigrate fileToMigrate;
    fileToMigrate.setFseq(1111);
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "store.add",
      store.add(fileToMigrate));

    castor::tape::tapegateway::FileMigratedNotification notfication;
    notfication.setFseq(1111);
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "store.markAsWrittenWithoutFlush",
      store.markAsWrittenWithoutFlush(notfication));

    std::list<castor::tape::tapegateway::FileMigratedNotification>
      notfications;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "store.getAndRemoveFilesWrittenWithoutFlush",
      notfications = store.getAndRemoveFilesWrittenWithoutFlush(1111));
  }

  CPPUNIT_TEST_SUITE(PendingMigrationsStoreTest);
  CPPUNIT_TEST(testGoodDayAddMarkGetAndRemove);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(PendingMigrationsStoreTest);

#endif // TEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
