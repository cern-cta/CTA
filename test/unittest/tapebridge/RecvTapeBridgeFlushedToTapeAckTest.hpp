/******************************************************************************
 *    test/unittest/tapebridge/RecvTapeBridgeFlushedToTapeAckTest.hpp
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

#ifndef TEST_UNITTEST_TAPEBRIDGE_RECVTAPEBRIDGEFLUSHEDTOTAPEACKTEST_HPP
#define TEST_UNITTEST_TAPEBRIDGE_RECVTAPEBRIDGEFLUSHEDTOTAPEACKTEST_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/legacymsg/TapeBridgeMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "h/Cuuid.h"
#include "h/marshall.h"
#include "h/net.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/tapebridge_recvTapeBridgeFlushedToTapeAck.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

#define FAKE_TAPEBRIDGE_LISTEN_PORT 64000

namespace castor     {
namespace tape       {
namespace tapebridge {

class RecvTapeBridgeFlushedToTapeAckTest: public CppUnit::TestFixture {
private:

  typedef struct {
    int  inListenSocketFd;
    bool outAcceptedConnection;
    bool outReadInAndUnmarshalledMsg;
  } tapebridgedThreadParams_t;

  static void *tapebridged_thread(void *arg) {
    const int netReadWriteTimeout = 60;
    tapebridgedThreadParams_t *const threadParams =
      (tapebridgedThreadParams_t*)arg;

    try {
      // Not successful until proven otherwise
      threadParams->outAcceptedConnection       = false;
      threadParams->outReadInAndUnmarshalledMsg = false;

      // Wrap the listen socket in a smart file-descriptor
      castor::tape::utils::SmartFd listenSockFd(threadParams->inListenSocketFd);

      // Accept incoming connection
      const time_t acceptTimeout = 10; // Timeout is in seconds
      castor::tape::utils::SmartFd connectionSockFd(
        castor::tape::net::acceptConnection(listenSockFd.get(), acceptTimeout));
      threadParams->outAcceptedConnection = true;

      // Read in and unmarshall the message
      tapeBridgeFlushedToTapeAckMsg_t msg;
      memset(&msg, '\0', sizeof(msg));
      const uint32_t recvRc = tapebridge_recvTapeBridgeFlushedToTapeAck(
        connectionSockFd.get(), netReadWriteTimeout, &msg);
      if(3 * LONGSIZE != recvRc || // magic + reqType + status
        RTCOPY_MAGIC != msg.magic ||
        TAPEBRIDGE_FLUSHEDTOTAPE != msg.reqType ||
        0 != msg.status) {
        test_exception te("failed to read in and unmarshall the message");
        throw te;
      }
      threadParams->outReadInAndUnmarshalledMsg = true;

      close(connectionSockFd.release());
      close(listenSockFd.release());
    } catch(...) {
      std::cerr << "Caught an exeption" << std::endl;
    }

    return arg;
  }

  int createListenerSock_stdException(const char *addr,
    const unsigned short lowPort, const unsigned short highPort,
    unsigned short &chosenPort) {
    try {
      return castor::tape::net::createListenerSock(addr, lowPort, highPort,
        chosenPort);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

      throw te;
    }
  }

public:
  void setUp() {
  }

  void tearDown() {
  }

  void testInvalidSocketFd() {
    const int invalidSocketFd = -10;
    const int netReadWriteTimeout = 60;
    tapeBridgeFlushedToTapeAckMsg_t msg;

    memset(&msg, '\0', sizeof(msg));
    const int rc = tapebridge_recvTapeBridgeFlushedToTapeAck(invalidSocketFd,
      netReadWriteTimeout, &msg);
    const int saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("rc for invalid socket file-descriptor",
      -1,
      rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for invalid socket file-descriptor",
      EINVAL,
      saved_serrno);
  }

  void testNullMsgBody() {
    const int validSocketFd = 0;
    const int netReadWriteTimeout = 60;

    const int rc = tapebridge_recvTapeBridgeFlushedToTapeAck(validSocketFd,
      netReadWriteTimeout, NULL);
    const int saved_serrno = serrno;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("rc for NULL msg",
      -1,
      rc);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for NULL msg",
      EINVAL,
      saved_serrno);
  }

  void testSendAndRecv() {
    const int netReadWriteTimeout = 60;

    // Create the TCP/IP listening-port
    castor::tape::utils::SmartFd listenSock;
    {
      const unsigned short lowPort    = FAKE_TAPEBRIDGE_LISTEN_PORT;
      const unsigned short highPort   = FAKE_TAPEBRIDGE_LISTEN_PORT;
      unsigned short       chosenPort = 0;

      CPPUNIT_ASSERT_NO_THROW_MESSAGE("create listening port",
        listenSock.reset(createListenerSock_stdException("127.0.0.1", lowPort,
          highPort, chosenPort)));

      CPPUNIT_ASSERT_EQUAL_MESSAGE("chosen listening port",
         (unsigned short)FAKE_TAPEBRIDGE_LISTEN_PORT, chosenPort);
    }

    // Create the TCP/IP server-thread
    tapebridgedThreadParams_t threadParams;
    memset(&threadParams, '\0', sizeof(threadParams));
    threadParams.inListenSocketFd = listenSock.release(); // Thread will close
    pthread_t tapebridged_thread_id;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_create", 0,
      pthread_create(&tapebridged_thread_id, NULL, tapebridged_thread,
      &threadParams));

    // Create client connection
    utils::SmartFd clientConnectionSock(socket(PF_INET, SOCK_STREAM,
      IPPROTO_TCP));
    CPPUNIT_ASSERT_MESSAGE("create client connection socket",
      0 <= clientConnectionSock.get());
    {
      struct sockaddr_in sockAddr;
      memset(&sockAddr, '\0', sizeof(sockAddr));
      sockAddr.sin_family      = AF_INET;
      sockAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
      sockAddr.sin_port        = htons(FAKE_TAPEBRIDGE_LISTEN_PORT);

      CPPUNIT_ASSERT_EQUAL_MESSAGE("create client connection",
        0,
        connect(clientConnectionSock.get(), (struct sockaddr *) &sockAddr,
          sizeof(sockAddr)));
    }

    // Send TAPEBRIDGE_FLUSHEDTOTAPE acknowledgement message using client
    // connection
    legacymsg::MessageHeader ackMsg;
    memset(&ackMsg, '\0', sizeof(ackMsg));
    ackMsg.magic       = RTCOPY_MAGIC;
    ackMsg.reqType     = TAPEBRIDGE_FLUSHEDTOTAPE;
    ackMsg.lenOrStatus = 0;
    LegacyTxRx legacyTxRx(netReadWriteTimeout);
    legacyTxRx.sendMsgHeader(clientConnectionSock.get(), ackMsg);

    void *tapebridged_thread_result = NULL;
      CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_join", 0,
        pthread_join(tapebridged_thread_id, &tapebridged_thread_result));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread argument in equals out",
      (void *)(&threadParams),
      tapebridged_thread_result);

    CPPUNIT_ASSERT_MESSAGE("threadParams.outAcceptedConnection",
      threadParams.outAcceptedConnection);
    CPPUNIT_ASSERT_MESSAGE("threadParams.outReadInAndUnmarshalledMsg",
      threadParams.outReadInAndUnmarshalledMsg);

    close(clientConnectionSock.release());
  }

  CPPUNIT_TEST_SUITE(RecvTapeBridgeFlushedToTapeAckTest);
  CPPUNIT_TEST(testInvalidSocketFd);
  CPPUNIT_TEST(testNullMsgBody);
  CPPUNIT_TEST(testSendAndRecv);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(RecvTapeBridgeFlushedToTapeAckTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_TAPEBRIDGE_RECVTAPEBRIDGEFLUSHEDTOTAPEACKTEST_HPP
