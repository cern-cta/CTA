/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/LegacyTxRxTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_LEGACYTXRXTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_LEGACYTXRXTEST_HPP 1

#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "h/serrno.h"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class LegacyTxRxTest: public CppUnit::TestFixture {
public:

  LegacyTxRxTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    const int netReadWriteTimeout = 12345;
    std::auto_ptr<LegacyTxRx> smartLegacyTxRx;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new LegacyTxRx()",
      smartLegacyTxRx.reset(new LegacyTxRx(netReadWriteTimeout)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of netReadWriteTimeout",
      netReadWriteTimeout,
      smartLegacyTxRx->getNetReadWriteTimeout());
  }

  void testSendAndRecieveMsgHeader() {
    legacymsg::MessageHeader msgHeaderToSend;
    msgHeaderToSend.magic       = 12;
    msgHeaderToSend.reqType     = 34;
    msgHeaderToSend.lenOrStatus = 56;

    int sockPair[2] = {-1, -1};
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Create socket pair",
      0,
      socketpair(PF_LOCAL, SOCK_STREAM, 0, sockPair));
    utils::SmartFd sendSock(sockPair[0]);
    utils::SmartFd recvSock(sockPair[1]);

    const int netReadWriteTimeout = 1;
    std::auto_ptr<LegacyTxRx> smartLegacyTxRx;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new LegacyTxRx()",
      smartLegacyTxRx.reset(new LegacyTxRx(netReadWriteTimeout)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of netReadWriteTimeout",
      netReadWriteTimeout,
      smartLegacyTxRx->getNetReadWriteTimeout());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check sendMsgHeader()",
      smartLegacyTxRx->sendMsgHeader(sendSock.get(), msgHeaderToSend));

    legacymsg::MessageHeader receivedMsgHeader;
    receivedMsgHeader.magic       = 0;
    receivedMsgHeader.reqType     = 0;
    receivedMsgHeader.lenOrStatus = 0;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check receiveMsgHeader()",
      smartLegacyTxRx->receiveMsgHeader(recvSock.get(), receivedMsgHeader));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check magic",
      msgHeaderToSend.magic,
      receivedMsgHeader.magic);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check reqType",
      msgHeaderToSend.reqType,
      receivedMsgHeader.reqType);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check lenOrStatus",
      msgHeaderToSend.lenOrStatus,
      receivedMsgHeader.lenOrStatus);
  }

  void testSendAndRecieveMsgHeaderFromCloseableWithNoClose() {
    legacymsg::MessageHeader msgHeaderToSend;
    msgHeaderToSend.magic       = 12;
    msgHeaderToSend.reqType     = 34;
    msgHeaderToSend.lenOrStatus = 56;

    int sockPair[2] = {-1, -1};
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Create socket pair",
      0,
      socketpair(PF_LOCAL, SOCK_STREAM, 0, sockPair));
    utils::SmartFd sendSock(sockPair[0]);
    utils::SmartFd recvSock(sockPair[1]);

    const int netReadWriteTimeout = 1;
    std::auto_ptr<LegacyTxRx> smartLegacyTxRx;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new LegacyTxRx()",
      smartLegacyTxRx.reset(new LegacyTxRx(netReadWriteTimeout)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of netReadWriteTimeout",
      netReadWriteTimeout,
      smartLegacyTxRx->getNetReadWriteTimeout());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check sendMsgHeader()",
      smartLegacyTxRx->sendMsgHeader(sendSock.get(), msgHeaderToSend));

    legacymsg::MessageHeader receivedMsgHeader;
    receivedMsgHeader.magic       = 0;
    receivedMsgHeader.reqType     = 0;
    receivedMsgHeader.lenOrStatus = 0;

    bool connectionClosed = false;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check receiveMsgHeaderFromCloseable()",
      connectionClosed = smartLegacyTxRx->receiveMsgHeaderFromCloseable(
        recvSock.get(), receivedMsgHeader));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check connection not closed",
      false,
      connectionClosed);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check magic",
      msgHeaderToSend.magic,
      receivedMsgHeader.magic);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check reqType",
      msgHeaderToSend.reqType,
      receivedMsgHeader.reqType);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check lenOrStatus",
      msgHeaderToSend.lenOrStatus,
      receivedMsgHeader.lenOrStatus);
  }

  void testSendAndRecieveMsgHeaderFromCloseableWithClose() {
    int sockPair[2] = {-1, -1};
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Create socket pair",
      0,
      socketpair(PF_LOCAL, SOCK_STREAM, 0, sockPair));
    utils::SmartFd sendSock(sockPair[0]);
    utils::SmartFd recvSock(sockPair[1]);

    const int netReadWriteTimeout = 1;
    std::auto_ptr<LegacyTxRx> smartLegacyTxRx;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new LegacyTxRx()",
      smartLegacyTxRx.reset(new LegacyTxRx(netReadWriteTimeout)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of netReadWriteTimeout",
      netReadWriteTimeout,
      smartLegacyTxRx->getNetReadWriteTimeout());

    close(sendSock.release());

    legacymsg::MessageHeader receivedMsgHeader;
    receivedMsgHeader.magic       = 0;
    receivedMsgHeader.reqType     = 0;
    receivedMsgHeader.lenOrStatus = 0;

    bool connectionClosed = false;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check receiveMsgHeaderFromCloseable()",
      connectionClosed = smartLegacyTxRx->receiveMsgHeaderFromCloseable(
        recvSock.get(), receivedMsgHeader));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check connection is closed",
      true,
      connectionClosed);
  }

  CPPUNIT_TEST_SUITE(LegacyTxRxTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testSendAndRecieveMsgHeader);
  CPPUNIT_TEST(testSendAndRecieveMsgHeaderFromCloseableWithNoClose);
  CPPUNIT_TEST(testSendAndRecieveMsgHeaderFromCloseableWithClose);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(LegacyTxRxTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_LEGACYTXRXTEST_HPP
