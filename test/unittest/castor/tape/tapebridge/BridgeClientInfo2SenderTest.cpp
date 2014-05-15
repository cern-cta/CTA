/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/BridgeClientInfo2SenderTest.hpp
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

#include "rtcp_recvRtcpHdr.h"
#include "h/marshall.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_GetClientInfo.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/vdqm_api.h"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/BridgeClientInfo2Sender.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <pthread.h>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAKE_RTCPD_LISTEN_PORT 65000

namespace castor     {
namespace tape       {
namespace tapebridge {

class BridgeClientInfo2SenderTest: public CppUnit::TestFixture {
private:

  tapeBridgeClientInfo2MsgBody_t m_clientInfoMsgBody;

  int createListenerSock_stdException(const char *addr,
    const unsigned short lowPort, const unsigned short highPort,
    unsigned short &chosenPort) {
    try {
      return net::createListenerSock(addr, lowPort, highPort,
        chosenPort);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

      throw te;
    }
  }

  struct rtcpd_thread_params {
    int                            inListenSocketFd;
    int32_t                        inMarshalledMsgBodyLen;
    int                            outGetClientInfo2Success;
    tapeBridgeClientInfo2MsgBody_t outMsgBody;
    bool                           outAnErrorOccurred;
    std::ostringstream             outErrorStream;

    rtcpd_thread_params():
      inListenSocketFd(-1),
      inMarshalledMsgBodyLen(0),
      outGetClientInfo2Success(0),
      outAnErrorOccurred(false) {
      memset(&outMsgBody, '\0', sizeof(outMsgBody));
    }
  };

  static void *rtcpd_thread(void *arg) {
    rtcpd_thread_params *const threadParams = (rtcpd_thread_params*)arg;

    try {
      if(NULL == threadParams) {
        test_exception te("Pointer to the thread-parameters is NULL");
        throw te;
      }

      utils::SmartFd listenSock(threadParams->inListenSocketFd);

      const time_t acceptTimeout = 10; // Timeout is in seconds
      utils::SmartFd connectionSockFd(
        net::acceptConnection(listenSock.get(), acceptTimeout));

      const int netReadWriteTimeout = 10; // Timeout is in seconds
      rtcpClientInfo_t client;
      rtcpTapeRequest_t tapeReq;
      rtcpFileRequest_t fileReq;
      int clientIsTapeBridge = 0;
      char errBuf[1024];
      rtcpd_GetClientInfo(
        connectionSockFd.get(),
        netReadWriteTimeout,
        &tapeReq,
        &fileReq,
        &client,
        &clientIsTapeBridge,
        &(threadParams->outMsgBody),
        errBuf,
        sizeof(errBuf));

      threadParams->outGetClientInfo2Success = 1;
    } catch(castor::exception::Exception &ce) {
      threadParams->outAnErrorOccurred = true;
      threadParams->outErrorStream <<
        "ERROR"
        ": " << __FUNCTION__ <<
        ": Caught a castor::exception::Exception"
        ": " << ce.getMessage().str() << std::endl;
    } catch(std::exception &se) {
      threadParams->outAnErrorOccurred = true;
      threadParams->outErrorStream <<
        "ERROR"
        ": " << __FUNCTION__ <<
        ": Caught an std::exception"
        ": " << se.what() << std::endl;
    } catch(...) {
      threadParams->outAnErrorOccurred = true;
      threadParams->outErrorStream <<
        "ERROR"
        ": " << __FUNCTION__ <<
        ": Caught an unknown exception";
    }

    return arg;
  }

public:

  BridgeClientInfo2SenderTest() {
    memset(&m_clientInfoMsgBody, '\0', sizeof(m_clientInfoMsgBody));
  }


  void test_submit_std_exception(legacymsg::RtcpJobReplyMsgBody
    &reply) throw(std::exception) {
    const std::string  rtcpdHost("127.0.0.1");;
    const unsigned int rtcpdPort = FAKE_RTCPD_LISTEN_PORT;
    const int          netReadWriteTimeout = 5; /* 5 seconds */

    try {
      BridgeClientInfo2Sender::send(
        rtcpdHost,
        rtcpdPort,
        netReadWriteTimeout,
        m_clientInfoMsgBody,
        reply);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

      throw te;
    }
  }

  void setUp() {
    setenv("PATH_CONFIG", "localhostIsAdminHost_castor.conf", 1);

    memset(&m_clientInfoMsgBody, '\0', sizeof(m_clientInfoMsgBody));

    m_clientInfoMsgBody.volReqId                 = 1111;
    m_clientInfoMsgBody.bridgeCallbackPort       = 2222;
    m_clientInfoMsgBody.bridgeClientCallbackPort = 3333;
    m_clientInfoMsgBody.clientUID                = 4444;
    m_clientInfoMsgBody.clientGID                = 5555;
    m_clientInfoMsgBody.tapeFlushMode            = TAPEBRIDGE_ONE_FLUSH_PER_N_FILES;
    m_clientInfoMsgBody.maxBytesBeforeFlush      = 7777;
    m_clientInfoMsgBody.maxFilesBeforeFlush      = 8888;
    utils::copyString(m_clientInfoMsgBody.bridgeHost      , "bridgeHost");
    utils::copyString(m_clientInfoMsgBody.bridgeClientHost, "bridgeClientHost");
    utils::copyString(m_clientInfoMsgBody.dgn             , "dgn");
    utils::copyString(m_clientInfoMsgBody.drive           , "drive");
    utils::copyString(m_clientInfoMsgBody.clientName      , "clientName");
  }

  void tearDown() {
  }

  void testSubmitZeroLengthRtcpdHost() {
    const std::string  rtcpdHost;
    const unsigned int rtcpdPort = 0;
    const int          netReadWriteTimeout = 0;
    legacymsg::RtcpJobReplyMsgBody reply;

    memset(&reply, '\0', sizeof(reply));

    CPPUNIT_ASSERT_THROW_MESSAGE("rtcpdHost = \"\"",
      BridgeClientInfo2Sender::send(
        rtcpdHost,
        rtcpdPort,
        netReadWriteTimeout,
        m_clientInfoMsgBody,
        reply),
      castor::exception::InvalidArgument);
  }

  void testSubmit() {
    rtcpd_thread_params threadParams;

    threadParams.inMarshalledMsgBodyLen =
      tapebridge_tapeBridgeClientInfo2MsgBodyMarshalledSize(
      &m_clientInfoMsgBody);
    CPPUNIT_ASSERT_MESSAGE(
      "tapebridge_tapeBridgeClientInfo2MsgBodyMarshalledSize",
      threadParams.inMarshalledMsgBodyLen > 0);

    utils::SmartFd listenSock;
    {
      const unsigned short lowPort    = FAKE_RTCPD_LISTEN_PORT;
      const unsigned short highPort   = FAKE_RTCPD_LISTEN_PORT;
      unsigned short       chosenPort = 0;

      // Create the TCP/IP listening-port
      CPPUNIT_ASSERT_NO_THROW_MESSAGE("create listening port",
        listenSock.reset(createListenerSock_stdException("127.0.0.1", lowPort,
          highPort, chosenPort)));

      CPPUNIT_ASSERT_EQUAL_MESSAGE("chosen listening port",
         (unsigned short)FAKE_RTCPD_LISTEN_PORT, chosenPort);
    }

    // Create the TCP/IP server-thread
    threadParams.inListenSocketFd = listenSock.release(); // Thread will close
    pthread_t rtcpd_thread_id;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_create", 0,
      pthread_create(&rtcpd_thread_id, NULL, rtcpd_thread, &threadParams));

    legacymsg::RtcpJobReplyMsgBody reply;
    memset(&reply, '\0', sizeof(reply));
    bool caughtSubmitException = false;
    test_exception submitException("UNKNOWN");
    try {
      test_submit_std_exception(reply);
    } catch(test_exception &te) {
      submitException = te;
      caughtSubmitException = true;
    }

    // To prevent a memory leak from using pthreads, join with the TCP/IP
    // server-thread before executing any cppunit asserts
    void *rtcpd_thread_result = NULL;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_join", 0,
      pthread_join(rtcpd_thread_id, &rtcpd_thread_result));

    if(caughtSubmitException) {
      CPPUNIT_ASSERT_NO_THROW_MESSAGE("GOOD DAY submit",
        throw submitException);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Thread results are same structure",
      (void *)&threadParams, rtcpd_thread_result);

    if(threadParams.outAnErrorOccurred) {
      test_exception te(threadParams.outErrorStream.str());
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "The rtcpd_thread encountered an error",
        throw te);
    }

    CPPUNIT_ASSERT_MESSAGE("Check rtcpd_GetClientInfo2",
      threadParams.outGetClientInfo2Success);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("volReqId",
      m_clientInfoMsgBody.volReqId, threadParams.outMsgBody.volReqId);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeCallbackPort",
      m_clientInfoMsgBody.bridgeCallbackPort,
      threadParams.outMsgBody.bridgeCallbackPort);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeClientCallbackPort",
      m_clientInfoMsgBody.bridgeClientCallbackPort,
      threadParams.outMsgBody.bridgeClientCallbackPort);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientUID",
      m_clientInfoMsgBody.clientUID, threadParams.outMsgBody.clientUID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientGID",
      m_clientInfoMsgBody.clientGID, threadParams.outMsgBody.clientGID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("tapeFlushMode",
      m_clientInfoMsgBody.tapeFlushMode,
      threadParams.outMsgBody.tapeFlushMode);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush",
      m_clientInfoMsgBody.maxBytesBeforeFlush,
      threadParams.outMsgBody.maxBytesBeforeFlush);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush",
      m_clientInfoMsgBody.maxFilesBeforeFlush,
      threadParams.outMsgBody.maxFilesBeforeFlush);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeHost",
      std::string(m_clientInfoMsgBody.bridgeHost),
      std::string(threadParams.outMsgBody.bridgeHost));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeClientHost",
      std::string(m_clientInfoMsgBody.bridgeClientHost),
      std::string(threadParams.outMsgBody.bridgeClientHost));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("dgn",
      std::string(m_clientInfoMsgBody.dgn),
      std::string(threadParams.outMsgBody.dgn));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("drive",
      std::string(m_clientInfoMsgBody.drive),
      std::string(threadParams.outMsgBody.drive));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientName",
      std::string(m_clientInfoMsgBody.clientName),
      std::string(threadParams.outMsgBody.clientName));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("ack status", 0, reply.status);
  }

  CPPUNIT_TEST_SUITE(BridgeClientInfo2SenderTest);
  CPPUNIT_TEST(testSubmitZeroLengthRtcpdHost);
  CPPUNIT_TEST(testSubmit);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeClientInfo2SenderTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
