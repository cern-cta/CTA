/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/MigrationReportConnectionTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTIONTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTIONTEST_HPP 1

#include "castor/tape/tapebridge/MigrationReportConnection.hpp"

#include <cppunit/extensions/HelperMacros.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class MigrationReportConnectionTest: public CppUnit::TestFixture {
public:

  MigrationReportConnectionTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    castor::tape::tapebridge::MigrationReportConnection connection;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check clientSock is initialised correctly",
      -1,
      connection.clientSock);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check aggregatorTransactionId is initialised correctly",
      (uint64_t)0,
      connection.aggregatorTransactionId);
  }

  CPPUNIT_TEST_SUITE(MigrationReportConnectionTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(MigrationReportConnectionTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTIONTEST_HPP
