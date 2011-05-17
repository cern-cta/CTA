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
  int                           outRtcpdHdrReadSuccess;
  int                           outMagicCorrect;
  int                           outReqTypeCorrect;
  int                           outMsgBodyLenCorrect;
  int                           outMsgBodyReadSuccess;
  int                           outMsgBodyUnmarshallSuccess;
  tapeBridgeClientInfoMsgBody_t outMsgBody;
  int                           outSendAckSuccess;
} rtcpd_thread_input_and_results_t;

void *rtcpd_thread(void *arg) {
  try {
    rtcpd_thread_input_and_results_t *threadResults =
      (rtcpd_thread_input_and_results_t*)arg;
    castor::tape::utils::SmartFd listenSock(threadResults->inListenSocketFd);
    int save_serrno = 0;

    const time_t acceptTimeout = 10; // Timeout is in seconds
    castor::tape::utils::SmartFd connectionSockFd(
      castor::tape::net::acceptConnection(listenSock.get(), acceptTimeout));

    // Read in the header of the rtcpd message from the VDQM or tape-bridge
    const int netReadWriteTimeout = 10; // Timeout is in seconds
    rtcpHdr_t rtcpHdr;
    memset(&rtcpHdr, '\0', sizeof(rtcpHdr));
    const int recvRc = rtcp_recvRtcpHdr(connectionSockFd.get(), &rtcpHdr,
      netReadWriteTimeout);
    save_serrno = serrno;
    threadResults->outRtcpdHdrReadSuccess = recvRc == 12;
    if(!threadResults->outRtcpdHdrReadSuccess) {
      return threadResults;
    }

    // Check the magic, reqtype and length entries in the header
    threadResults->outMagicCorrect = rtcpHdr.magic == RTCOPY_MAGIC_OLD0;
    if(!threadResults->outMagicCorrect) {
      return threadResults;
    }
    threadResults->outReqTypeCorrect = rtcpHdr.reqtype == TAPEBRIDGE_CLIENTINFO;
    if(!threadResults->outReqTypeCorrect) {
      return threadResults;
    }
    threadResults->outMsgBodyLenCorrect =
      rtcpHdr.len == threadResults->inMarshalledMsgBodyLen;
    if(!threadResults->outMsgBodyLenCorrect) {
      return threadResults;
    }

    // Read in the body of the message
    char buf[RTCP_MSGBUFSIZ];
    const int netreadMsgBodyRc = netread_timeout(connectionSockFd.get(), buf,
      rtcpHdr.len, netReadWriteTimeout);
    threadResults->outMsgBodyReadSuccess = netreadMsgBodyRc == rtcpHdr.len;
    if(!threadResults->outMsgBodyReadSuccess) {
      return threadResults;
    }

    // Unmarshall the body into the C structure
    memset(&(threadResults->outMsgBody), '\0',
      sizeof(threadResults->outMsgBody));
    const int32_t unmarshallRc =
      tapebridge_unmarshallTapeBridgeClientInfoMsgBody(buf, sizeof(buf),
      &(threadResults->outMsgBody));
    threadResults->outMsgBodyUnmarshallSuccess = unmarshallRc == rtcpHdr.len;
    if(!threadResults->outMsgBodyUnmarshallSuccess) {
      return threadResults;
    }

    // Acknowledge the message
    const uint32_t    ackStatus = 1234;
    const char *const ackErrMsg = "errorMessage";
    const int sendAckRc = tapebridge_sendTapeBridgeAck(connectionSockFd.get(),
      netReadWriteTimeout, ackStatus, ackErrMsg);
    threadResults->outSendAckSuccess = sendAckRc != -1;
    if(!threadResults->outSendAckSuccess) {
      return threadResults;
    }
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
    rtcpd_thread_input_and_results_t threadResults;

    memset(&threadResults, '\0', sizeof(threadResults));
    threadResults.inMarshalledMsgBodyLen =
      tapebridge_tapeBridgeClientInfoMsgBodyMarshalledSize(
      &m_clientInfoMsgBody);
    CPPUNIT_ASSERT_MESSAGE(
      "tapebridge_tapeBridgeClientInfoMsgBodyMarshalledSize",
      threadResults.inMarshalledMsgBodyLen > 0);

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
    threadResults.inListenSocketFd = listenSock.release(); // Thread will close
    pthread_t rtcpd_thread_id;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_create", 0,
      pthread_create(&rtcpd_thread_id, NULL, rtcpd_thread, &threadResults));

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
      (void *)&threadResults, rtcpd_thread_result);

    CPPUNIT_ASSERT_MESSAGE("Check rtcpd header read",
      threadResults.outRtcpdHdrReadSuccess);
    CPPUNIT_ASSERT_MESSAGE("Check message magic",
      threadResults.outMagicCorrect);
    CPPUNIT_ASSERT_MESSAGE("Check message reqType",
      threadResults.outReqTypeCorrect);
    CPPUNIT_ASSERT_MESSAGE("Check message body length",
      threadResults.outMsgBodyLenCorrect);
    CPPUNIT_ASSERT_MESSAGE("Check message body read success",
      threadResults.outMsgBodyReadSuccess);
    CPPUNIT_ASSERT_MESSAGE("Check message body unmarshall success",
      threadResults.outMsgBodyUnmarshallSuccess);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("volReqId",
      m_clientInfoMsgBody.volReqId, threadResults.outMsgBody.volReqId);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeCallbackPort",
      m_clientInfoMsgBody.bridgeCallbackPort,
      threadResults.outMsgBody.bridgeCallbackPort);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeClientCallbackPort",
      m_clientInfoMsgBody.bridgeClientCallbackPort,
      threadResults.outMsgBody.bridgeClientCallbackPort);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientUID",
      m_clientInfoMsgBody.clientUID, threadResults.outMsgBody.clientUID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientGID",
      m_clientInfoMsgBody.clientGID, threadResults.outMsgBody.clientGID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles",
      m_clientInfoMsgBody.useBufferedTapeMarksOverMultipleFiles,
      threadResults.outMsgBody.useBufferedTapeMarksOverMultipleFiles);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeHost",
      std::string(m_clientInfoMsgBody.bridgeHost),
      std::string(threadResults.outMsgBody.bridgeHost));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bridgeClientHost",
      std::string(m_clientInfoMsgBody.bridgeClientHost),
      std::string(threadResults.outMsgBody.bridgeClientHost));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("dgn",
      std::string(m_clientInfoMsgBody.dgn),
      std::string(threadResults.outMsgBody.dgn));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("drive",
      std::string(m_clientInfoMsgBody.drive),
      std::string(threadResults.outMsgBody.drive));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientName",
      std::string(m_clientInfoMsgBody.clientName),
      std::string(threadResults.outMsgBody.clientName));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("ack status", (uint32_t)1234, reply.status);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("ack errorMessage",
      std::string("errorMessage"),
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
