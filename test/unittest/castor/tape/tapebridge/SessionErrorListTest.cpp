/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/SessionErrorListTest.hpp
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

#include "castor/tape/tapebridge/SessionError.hpp"
#include "castor/tape/tapebridge/SessionErrorList.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

class SessionErrorListTest: public CppUnit::TestFixture {
public:

  SessionErrorListTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    std::auto_ptr<SessionErrorList>     smartErrorList;
    const Cuuid_t                       &cuuid = nullCuuid;
    const uint32_t                      mountTransactionId = 1234;
    const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        mountTransactionId, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };
    const tapegateway::Volume           volume;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new SessionErrorList()",
      smartErrorList.reset(new SessionErrorList(cuuid, jobRequest, volume)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check newly created list is empty",
      (SessionErrorList::size_type)0,
      smartErrorList->size());
  }

  void testPush_back() {
    std::auto_ptr<SessionErrorList> smartErrorList;
    const Cuuid_t                       &cuuid = nullCuuid;
    const uint32_t                      mountTransactionId = 1234;
    const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        mountTransactionId, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };
    const tapegateway::Volume           volume;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new SessionErrorList()",
      smartErrorList.reset(new SessionErrorList(cuuid, jobRequest, volume)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check newly created list is empty",
      (SessionErrorList::size_type)0,
      smartErrorList->size());

    SessionError sessionError;
    sessionError.setErrorCode(1234);
    sessionError.setErrorMessage("Test error message");

    smartErrorList->push_back(sessionError);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check size of list after single push_back",
      (SessionErrorList::size_type)1,
      smartErrorList->size());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check error code at front of session error list",
      sessionError.getErrorCode(),
      smartErrorList->front().getErrorCode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check error message at front of session error list",
      sessionError.getErrorMessage(),
      smartErrorList->front().getErrorMessage());
  }

  CPPUNIT_TEST_SUITE(SessionErrorListTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testPush_back);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(SessionErrorListTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
