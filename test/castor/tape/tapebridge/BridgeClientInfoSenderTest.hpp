/******************************************************************************
 *                test/castor/tape/tapebridge/BridgeClientInfoSenderTest.hpp
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

#ifndef TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGECLIENTINFOSUBMITTERTEST_HPP
#define TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGECLIENTINFOSUBMITTERTEST_HPP 1

#include "rtcp_recvRtcpHdr.h"
#include "test_exception.hpp"
#include "h/marshall.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_GetClientInfo.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/tapebridge_sendTapeBridgeAck.h"
#include "h/vdqm_api.h"
#include "castor/exception/Internal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/BridgeClientInfoSender.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define FAKE_RTCPD_LISTEN_PORT 65000

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

typedef struct {
  int                           inListenSocketFd;
  int32_t                       inMarshalledMsgBodyLen;
  int                           outGetClientInfoSuccess;
  tapeBridgeClientInfoMsgBody_t outMsgBody;
} server_thread_params;

void *rtcpd_thread(void *arg) {
  try {
    server_thread_params *threadParams =
      (server_thread_params*)arg;
    castor::tape::utils::SmartFd listenSock(threadParams->inListenSocketFd);

    const time_t acceptTimeout = 10; // Timeout is in seconds
    castor::tape::utils::SmartFd connectionSockFd(
      castor::tape::net::acceptConnection(listenSock.get(), acceptTimeout));

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
    threadParams->outGetClientInfoSuccess = 1;
  } catch(castor::exception::Exception &ce) {
    std::cerr <<
      "ERROR"
      ": rtcpd_thread"
      ": Caught a castor::exception::Exception"
      ": " << ce.getMessage().str() << std::endl;
  } catch(std::exception &se) {
    std::cerr <<
      "ERROR"
      ": rtcpd_thread"
      ": Caught an std::exception"
      ": " << se.what() << std::endl;
  } catch(...) {
    std::cerr << 
      "ERROR"
      ": rtcpd_thread"
      ": Caught an unknown exception";
  }

  return arg;
}

class BridgeClientInfoSenderTest: public CppUnit::TestFixture {
private:

  tapeBridgeClientInfoMsgBody_t m_clientInfoMsgBody;

public:

  BridgeClientInfoSenderTest() {
    memset(&m_clientInfoMsgBody, '\0', sizeof(m_clientInfoMsgBody));
  }


  void test_submit_std_exception(castor::tape::legacymsg::RtcpJobReplyMsgBody
    &reply) throw(std::exception) {
    const std::string  rtcpdHost("127.0.0.1");;
    const unsigned int rtcpdPort = FAKE_RTCPD_LISTEN_PORT;
    const int          netReadWriteTimeout = 0;

    try {
      castor::tape::tapebridge::BridgeClientInfoSender::send(
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
    using namespace castor::tape;

    memset(&m_clientInfoMsgBody, '\0', sizeof(m_clientInfoMsgBody));

    m_clientInfoMsgBody.volReqId                              = 1111;
    m_clientInfoMsgBody.bridgeCallbackPort                    = 2222;
    m_clientInfoMsgBody.bridgeClientCallbackPort              = 3333;
    m_clientInfoMsgBody.clientUID                             = 4444;
    m_clientInfoMsgBody.clientGID                             = 5555;
    m_clientInfoMsgBody.useBufferedTapeMarksOverMultipleFiles = 6666;
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
    castor::tape::legacymsg::RtcpJobReplyMsgBody reply;

    memset(&reply, '\0', sizeof(reply));

    CPPUNIT_ASSERT_THROW_MESSAGE("rtcpdHost = \"\"",
      castor::tape::tapebridge::BridgeClientInfoSender::send(
        rtcpdHost,
        rtcpdPort,
        netReadWriteTimeout,
        m_clientInfoMsgBody,
        reply),
      castor::exception::InvalidArgument);
  }

  void testSubmitUnknownRtcpdHost() {
    const std::string  rtcpdHost("UNKNOWN");
    const unsigned int rtcpdPort = 0;
    const int          netReadWriteTimeout = 0;
    castor::tape::legacymsg::RtcpJobReplyMsgBody reply;

    memset(&reply, '\0', sizeof(reply));

    CPPUNIT_ASSERT_THROW_MESSAGE("rtcpdHost = \"UNKNOWN\"",
      castor::tape::tapebridge::BridgeClientInfoSender::send(
        rtcpdHost,
        rtcpdPort,
        netReadWriteTimeout,
        m_clientInfoMsgBody,
        reply),
      castor::exception::Exception);
  }

  void testSubmit() {
    server_thread_params threadParams;

    memset(&threadParams, '\0', sizeof(threadParams));
    threadParams.inMarshalledMsgBodyLen =
      tapebridge_tapeBridgeClientInfoMsgBodyMarshalledSize(
      &m_clientInfoMsgBody);
    CPPUNIT_ASSERT_MESSAGE(
      "tapebridge_tapeBridgeClientInfoMsgBodyMarshalledSize",
      threadParams.inMarshalledMsgBodyLen > 0);

    castor::tape::utils::SmartFd listenSock;
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

    castor::tape::legacymsg::RtcpJobReplyMsgBody reply;
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

    CPPUNIT_ASSERT_MESSAGE("Check rtcpd_GetClientInfo",
      threadParams.outGetClientInfoSuccess);
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
    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles",
      m_clientInfoMsgBody.useBufferedTapeMarksOverMultipleFiles,
      threadParams.outMsgBody.useBufferedTapeMarksOverMultipleFiles);
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

    CPPUNIT_ASSERT_EQUAL_MESSAGE("ack status", -1, reply.status);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("ack errorMessage",
      std::string("Buffered tape-marks over multiple files is not supported"),
      std::string(reply.errorMessage));
  }

  CPPUNIT_TEST_SUITE(BridgeClientInfoSenderTest);
  CPPUNIT_TEST(testSubmitZeroLengthRtcpdHost);
  CPPUNIT_TEST(testSubmitUnknownRtcpdHost);
  CPPUNIT_TEST(testSubmit);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeClientInfoSenderTest);

#endif // TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGECLIENTINFOSUBMITTERTEST_HPP
