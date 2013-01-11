/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/GetMoreWorkConnectionTest.hpp
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

#include "castor/tape/tapebridge/GetMoreWorkConnection.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

class GetMoreWorkConnectionTest: public CppUnit::TestFixture {
public:

  GetMoreWorkConnectionTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    castor::tape::tapebridge::GetMoreWorkConnection connection;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpdSock is initialised correctly",
      -1,
      connection.rtcpdSock);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpdReqMagic is initialised correctly",
      (uint32_t)0,
      connection.rtcpdReqMagic);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpdReqType is initialised correctly",
      (uint32_t)0,
      connection.rtcpdReqType);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpdReqTapePath is initialised correctly",
      std::string(""),
      connection.rtcpdReqTapePath);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check clientSock is initialised correctly",
      -1,
      connection.clientSock);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check tapebridgeTransId is initialised correctly",
      (uint64_t)0,
      connection.tapebridgeTransId);

    {
      struct timeval clientReqTimeStamp = {0, 0};
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check clientReqTimeStamp.tv_sec is initialised correctly",
        clientReqTimeStamp.tv_sec,
        connection.clientReqTimeStamp.tv_sec);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check clientReqTimeStamp.tv_usec is initialised correctly",
        clientReqTimeStamp.tv_usec,
        connection.clientReqTimeStamp.tv_usec);
    }
  }

  CPPUNIT_TEST_SUITE(GetMoreWorkConnectionTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(GetMoreWorkConnectionTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
