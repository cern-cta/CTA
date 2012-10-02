/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/BridgeProtocolEngineTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP 1

#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/ClientProxy.hpp"
#include "castor/tape/tapebridge/ClientAddressLocal.hpp"
#include "castor/tape/tapebridge/FileWrittenNotificationList.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "h/Castor_limits.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_sendTapeBridgeFlushedToTape.h"
#include "h/tapebridge_recvTapeBridgeFlushedToTapeAck.h"
#include "test/unittest/castor/tape/tapebridge/DummyClientProxy.hpp"
#include "test/unittest/castor/tape/tapebridge/TestingBridgeProtocolEngine.hpp"
#include "test/unittest/castor/tape/tapebridge/TestingBulkRequestConfigParams.hpp"
#include "test/unittest/castor/tape/tapebridge/TestingTapeFlushConfigParams.hpp"
#include "test/unittest/castor/tape/tapebridge/TraceableDummyFileCloser.hpp"
#include "test/unittest/castor/tape/tapebridge/TraceableSystemFileCloser.hpp"
#include "test/unittest/unittest.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <iostream>
#include <pthread.h>
#include <memory>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class BridgeProtocolEngineTest: public CppUnit::TestFixture {
private:

  /**
   * The path of the local socket that the client would listen on for incoming
   * connections from the tapegatewayd daemon.
   */
  const char *const m_clientListenSockPath;

  /**
   * The path of the local socket on which the BridgeProtocolEngine will listen
   * for incoming connections from the rtcpd daemon.
   */
  const char *const m_bridgeListenSockPath;

  /**
   * The local-domain socket that the client would listen on for incoming
   * connections from the tapegatewayd daemon.
   */
  int m_clientListenSock;

  /**
   * The local-domain socket that the BridgeProtocolEngine will listen
   * for incoming connections from the rtcpd daemon.
   */
  int m_bridgeListenSock;

  /**
   * The rtcpd daemon side of the initial rtcpd-connection with the tapebridged
   * daemon.
   */
  int m_initialRtcpdSockRtcpdSide;

  /**
   * The tapebridged daemon side of the initial rtcpd-connection with the
   * tapebridged daemon.
   */
  int m_initialRtcpdSockBridgeSide;

  /**
   * The rtcpd daemon side of the IO control connection.
   */
  int m_ioControlConnectionSock;

  /**
   * The unique universal-identifier to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The volume identifier of the tape to be mounted.
   */
  const std::string m_volumeVid;

  /**
   * The density of the tape to be mounted.
   */
  const std::string m_volumeDensity;

  /**
   * The label of the tape to be mounted.
   */
  const std::string m_volumeLabel;

  /**
   * The device-group name of the tape to be mounted.
   */
  const std::string m_volumeDgn;

  /**
   * The name of the drive unit.
   */
  const std::string m_driveUnit;

  /**
   * The job-request structure sent from the vdqmd daemon.
   */
  legacymsg::RtcpJobRqstMsgBody m_jobRequest;

  /**
   * The volume message from the client of the tapebridged demon.
   */
  tapegateway::Volume m_volume;

  /**
   * The counter used to generate tape-bridge transaction ids.
   */
  Counter<uint64_t> m_tapebridgeTransactionCounter;

  /**
   * The tapebridged daemon configuration parameters that specifiy the
   * the bulk requests the daemon shall send to the tapegatewayd daemon.
   */
  TestingBulkRequestConfigParams m_bulkRequestConfigParams;

  /**
   * The tapebridged daemon configuration parameters that specifiy the
   * the tape-drive flush-logic.
   */
  TestingTapeFlushConfigParams m_tapeFlushConfigParams;

  /**
   * The file-closer to be ised by the BridgeProtocolEngine.
   */
  TraceableSystemFileCloser m_fileCloser;

  /**
   * The mount transaction id.
   */
  const uint32_t m_mountTransactionId;

  /**
   * The timeout in seconds for network operations.
   */
  const int m_netTimeout;

  /**
   * The address of the client of the tapebridged daemon.
   */
  ClientAddressLocal m_clientAddress;

  /**
   * The proxy-object representing the client of the tapebridged daemon.
   */
  ClientProxy m_clientProxy;

  /**
   * The number of files on the destination tape at the beginning of the mount.
   */
  const uint32_t m_nbFilesOnDestinationTape;

  /**
   * A castor::tape::utils::BoolFunctor that always returns false.
   */
  class AlwaysFalseBoolFunctor: public utils::BoolFunctor {
  public:

    bool operator()() const {
      return false;
    }

    ~AlwaysFalseBoolFunctor() throw() {
       // Do nothing
     }
  };

  /**
   * Functor that returns true if the tapebridged daemon should stop
   * gracefully.
   */
  AlwaysFalseBoolFunctor m_stoppingGracefully;

  /**
   * Object responisble for sending and receiving the header of messages
   * belonging to the legacy RTCOPY-protocol.
   */
  LegacyTxRx m_legacyTxRx;

  /**
   * Pointer to the BridgeProtocolEngine.
   */
  TestingBridgeProtocolEngine *m_engine;

  /**
   * The id of the mount or volume request.
   */
  const uint32_t m_volReqId;

  /**
   * The list of thread with which the main thread should join with when
   * tearing down a test.
   */
  std::list<pthread_t> m_threadsToJoinWithAtTearDown;

  /**
   * Creates a new ClientProxy object converting any thrown exceptions to a
   * std::exception.
   */
  ClientProxy *newClientProxy(
    const Cuuid_t          &cuuid,
    const uint32_t         mountTransactionId,
    const int              netTimeout,
    const ClientAddress    &clientAddress,
    const std::string      &dgn,
    const std::string      &driveUnit
  ) throw(std::exception) {
    try {
      return new ClientProxy(
        cuuid,
        mountTransactionId,
        netTimeout,
        clientAddress,
        dgn,
        driveUnit);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception");
      throw te;
    }
  }

  /**
   * Structure used to pass parameters to the test BridgeProtocolEngine-thread
   * responsible for starting an a session with the
   * rtcpd session by calling the BridgeProtocolEngine::run() method.
   */
  struct StartRtcpdSessionThreadParams {
    TestingBridgeProtocolEngine *inEngine;
    bool                        outAnErrorOccurred;
    std::ostringstream          outErrorStream;

    StartRtcpdSessionThreadParams():
      inEngine(NULL),
      outAnErrorOccurred(false) {
      // Do nothing
    }
  };

  /**
   * The test BridgeProtocolEngine-thread that is responsible for starting an
   * a session with the rtcpd session by calling the
   * BridgeProtocolEngine::run() method.
   */
  static void *runStartRtcpdSessionThread(void *const arg) {
    StartRtcpdSessionThreadParams *const threadParams =
      (StartRtcpdSessionThreadParams*)arg;

    try {
      if(NULL == threadParams) {
        test_exception te("Pointer to the thread-parameters is NULL");
        throw te;
      }

      if(NULL == threadParams->inEngine) {
        test_exception te("Pointer to the bridge protocol-engine is NULL");
        throw te;
      }

      threadParams->inEngine->run();
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

    try {
      delete castor::BaseObject::services();
    } catch(...) {
      // Ignore any exception
    }

    return arg;
  } // runStartRtcpdSessionThread

  /**
   * Structure used to pass parameters to the test rtcpd-thread responsible
   * for receiving the messages representing "no more work" from the
   * BridgeProtocolEngine.
   */
  struct ReceiveNoMoreWorkFromBridgeThreadParams {
    int                inIoControlConnectionSock;
    LegacyTxRx         *inLegacyTxRx;
    bool               outAnErrorOccurred;
    std::ostringstream outErrorStream;

    ReceiveNoMoreWorkFromBridgeThreadParams():
      inIoControlConnectionSock(-1),
      inLegacyTxRx(NULL),
      outAnErrorOccurred(false) {
      // Do nothing
    }
  }; // struct ReceiveNoMoreWorkFromBridgeThreadParams


  /**
   * The test rtcpd-thread that is responsible for receiving and replying to
   * the messages representing "no more work" from the BridgeProtocolEngine.
   *
   * The thread carries out the following snippet of the RTCOPY protocol.
   *
   * RTCPD            BRIDGE
   *   |                 |
   *   | RTCP_NOMORE_REQ |
   *   |<----------------|
   *   |                 |
   *   |     Ack         |
   *   |---------------->|
   *   |                 |
   *   |   Delayed ack   |
   *   |<----------------|
   *   |                 |
   *   |                 |
   *   |                 |
   *   |                 |
   */
  static void *receiveNoMoreWorkFromBridge(void *const arg) {
    ReceiveNoMoreWorkFromBridgeThreadParams *const threadParams =
      (ReceiveNoMoreWorkFromBridgeThreadParams*)arg;

    try {
      if(NULL == threadParams) {
        test_exception te("Pointer to the thread-parameters is NULL");
        throw te;
      }

      if(-1 == threadParams->inIoControlConnectionSock) {
        test_exception te(
          "Socket-descriptor of the IO control connection is invalid");
        throw te;
      }

      if(NULL == threadParams->inLegacyTxRx) {
        test_exception te("Pointer to the LegacyTxRx is NULL");
        throw te;
      }

      // Read in and unmarshal the RTCP_NOMORE_REQ message
      legacymsg::MessageHeader noMoreMsg;
      noMoreMsg.magic       = 0;
      noMoreMsg.reqType     = 0;
      noMoreMsg.lenOrStatus = 0;
      {
        const int netReadWriteTimeout = 1;
        char msgBuf[3 * sizeof(uint32_t)];
        memset(msgBuf, '\0', sizeof(msgBuf));
        net::readBytes(threadParams->inIoControlConnectionSock,
          netReadWriteTimeout, sizeof(msgBuf), msgBuf);
        const char *srcPtr = msgBuf;
        size_t srcLen = sizeof(msgBuf);
        legacymsg::unmarshal(srcPtr, srcLen, noMoreMsg);
      }

      if(RTCOPY_MAGIC != noMoreMsg.magic) {
        test_exception
          te("RTCP_NOMORE_REQ message has an invalid magic-number");
        throw te;
      }
      if(RTCP_NOMORE_REQ != noMoreMsg.reqType) {
        test_exception
          te("RTCP_NOMORE_REQ message has an invalid request-type");
        throw te;
      }
      if(0 != noMoreMsg.lenOrStatus) {
        test_exception 
          te("RTCP_NOMORE_REQ message has an invalid lenOrStatus");
        throw te;
      }

      // Send back an ACK of the RTCP_NOMORE_REQ message
      try {
        legacymsg::MessageHeader ackMsg;
        ackMsg.magic       = RTCOPY_MAGIC;
        ackMsg.reqType     = RTCP_NOMORE_REQ;
        ackMsg.lenOrStatus = 0;

        threadParams->inLegacyTxRx->sendMsgHeader(
          threadParams->inIoControlConnectionSock, ackMsg);
      } catch(castor::exception::Exception &ex) {
        test_exception
          te("Failed to send ack of RTCP_NOMORE_REQ message");
        throw te;
      }

      {
        // Read in and un-marshaled the delayed ACK of the request for more work
        legacymsg::MessageHeader ackMsg;
        memset(&ackMsg, '\0', sizeof(ackMsg));

        try {
          threadParams->inLegacyTxRx->receiveMsgHeader(
            threadParams->inIoControlConnectionSock, ackMsg);
        } catch(castor::exception::Exception &ex) {
          test_exception
            te("Failed to read in and un-marshal the delayed ACK of the request"
              " for more work");
          throw te;
        }

        // Check the contents of the delayed ACK of the request for more work
        if(RTCOPY_MAGIC != ackMsg.magic) {
          std::ostringstream oss;
          oss <<
            "Delayed ACK of request for more work contains an invalid"
            " magic-number"
            ": expected=0x" << std::hex << RTCOPY_MAGIC <<
            " actual=0x" << ackMsg.magic;
          test_exception te(oss.str());
          throw te;
        }
        if(RTCP_FILEERR_REQ != ackMsg.reqType) {
          std::ostringstream oss;
          oss <<
            "Delayed ACK of request for more work contains an invalid"
            " request-type"
            ": expected=0x" << std::hex << RTCP_FILEERR_REQ <<
            " actual=0x" << ackMsg.reqType;
          test_exception te(oss.str());
          throw te;
        }
        if(0 != ackMsg.lenOrStatus) {
          std::ostringstream oss;
          oss << 
            "Delayed ACK of request for more work contains an invalid"          
            " status"
            ": expected=0 actual=" << ackMsg.lenOrStatus;
          test_exception te(oss.str());
          throw te;
        }
      }
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

    try {
      delete castor::BaseObject::services();
    } catch(...) {
      // Ignore any exception
    }

    return arg;
  } // receiveNoMoreWorkFromBridge

public:

  /**
   * Constructor.
   */
  BridgeProtocolEngineTest():
    m_clientListenSockPath("/tmp/clientListenSockForBridgeProtocolEngineTest"),
    m_bridgeListenSockPath("/tmp/brigdeListenSockForBridgeProtocolEngineTest"),
    m_clientListenSock(0),
    m_bridgeListenSock(0),
    m_initialRtcpdSockRtcpdSide(0),
    m_initialRtcpdSockBridgeSide(0),
    m_ioControlConnectionSock(0),
    m_cuuid(nullCuuid),
    m_volumeVid("vid"),
    m_volumeDensity("density"),
    m_volumeLabel("label"),
    m_volumeDgn("dgn"),
    m_driveUnit("unit"),
    m_tapebridgeTransactionCounter(0),
    m_mountTransactionId(5678),
    m_netTimeout(2),
    m_clientAddress(m_clientListenSockPath),
    m_clientProxy(
      m_cuuid,
      m_mountTransactionId,
      m_netTimeout,
      m_clientAddress,
      m_volumeDgn,
      m_driveUnit),
    m_nbFilesOnDestinationTape(2),
    m_legacyTxRx(m_netTimeout),
    m_engine(0),
    m_volReqId(m_mountTransactionId) {
    memset(&m_jobRequest, 0, sizeof(m_jobRequest));
  }

  void setUp() {
    // Make sure the local-domain socket-files are deleted
    unlink(m_clientListenSockPath);
    unlink(m_bridgeListenSockPath);

    // Create the socket on which the client would listen for incoming
    // connections from the BridgeProtocolEngine
    m_clientListenSock =
      unittest::createLocalListenSocket(m_clientListenSockPath);

    // Create the socket on which the BridgeProtocolEngine will listen for
    // incoming connections from the rtcpd daemon
    m_bridgeListenSock =
      unittest::createLocalListenSocket(m_bridgeListenSockPath);

    // Create the initial rtcpd-connection using socketpair()
    int initialRtcpdSockPair[2] = {-1, -1};
    socketpair(PF_LOCAL, SOCK_STREAM, 0, initialRtcpdSockPair);
    m_initialRtcpdSockRtcpdSide  = initialRtcpdSockPair[0];
    m_initialRtcpdSockBridgeSide = initialRtcpdSockPair[1];

    // Create a socket for the disk/tape I/O control-connection
    m_ioControlConnectionSock = socket(PF_LOCAL, SOCK_STREAM, 0);

    // Initialise the job-request structure
    m_jobRequest.volReqId   = m_volReqId;
    m_jobRequest.clientPort = 0;
    m_jobRequest.clientEuid = 0;
    m_jobRequest.clientEgid = 0;
    memset(m_jobRequest.clientHost, '\0', sizeof(m_jobRequest.clientHost));
    strncpy(m_jobRequest.clientHost, "clientHost",
      sizeof(m_jobRequest.clientHost) - 1);
    memset(m_jobRequest.dgn, '\0', sizeof(m_jobRequest.dgn));
    strncpy(m_jobRequest.dgn, m_volumeDgn.c_str(),
      sizeof(m_jobRequest.dgn) - 1);
    memset(m_jobRequest.driveUnit, '\0', sizeof(m_jobRequest.driveUnit));
    strncpy(m_jobRequest.driveUnit, m_driveUnit.c_str(),
      sizeof(m_jobRequest.driveUnit) - 1);
    memset(m_jobRequest.clientUserName, '\0',
      sizeof(m_jobRequest.clientUserName));
    strncpy(m_jobRequest.clientUserName, "user",
      sizeof(m_jobRequest.clientUserName) - 1);

    // Initialise the volume message for migrations
    m_volume.setVid(m_volumeVid);
    m_volume.setDensity(m_volumeDensity);
    m_volume.setLabel(m_volumeLabel);
    m_volume.setMode(tapegateway::WRITE);
    m_volume.setClientType(tapegateway::TAPE_GATEWAY);

    // Reset the tapebridgeTransactionCounter
    m_tapebridgeTransactionCounter.reset(0);

    // Initialise the BulkRequestConfigParams
    m_bulkRequestConfigParams.setBulkRequestMigrationMaxBytes(1,
      ConfigParamSource::UNDEFINED);
    m_bulkRequestConfigParams.setBulkRequestMigrationMaxFiles(2,
      ConfigParamSource::UNDEFINED);
    m_bulkRequestConfigParams.setBulkRequestRecallMaxBytes(3,
      ConfigParamSource::UNDEFINED);
    m_bulkRequestConfigParams.setBulkRequestRecallMaxFiles(4,
      ConfigParamSource::UNDEFINED);

    // Initialise the TapeFlushConfigParams
    m_tapeFlushConfigParams.setTapeFlushMode(TAPEBRIDGE_N_FLUSHES_PER_FILE,
      ConfigParamSource::UNDEFINED);
    m_tapeFlushConfigParams.setMaxBytesBeforeFlush(1,
      ConfigParamSource::UNDEFINED);
    m_tapeFlushConfigParams.setMaxFilesBeforeFlush(1,
      ConfigParamSource::UNDEFINED);

    // Clear the list of threads to join with at tearDown
    m_threadsToJoinWithAtTearDown.clear();
  }

  void tearDown() {
    // Join with any thread created during the test that has not yet been
    // joined with
    for(std::list<pthread_t>::const_iterator itor =
      m_threadsToJoinWithAtTearDown.begin();
      itor != m_threadsToJoinWithAtTearDown.end();
      itor++) {
      pthread_join(*itor, NULL);
    }

    // Delete the BridgeProtocolEngine
    delete m_engine;

    // Clear the TraceableSystemFileCloser
    m_fileCloser.m_closedFds.clear();

    // Close all sockets accept those owned by the BridgeProtocolEngine
    close(m_ioControlConnectionSock);
    close(m_initialRtcpdSockRtcpdSide);
    close(m_bridgeListenSock);
    close(m_clientListenSock);

    // Make sure the local-domain socket-files are deleted
    unlink(m_clientListenSockPath);
    unlink(m_bridgeListenSockPath);
  }

  /**
   * Creates an instance of the TestingBridgeProtocolEngine class, converting
   * any thrown exceptions to std::exception.
   */
  TestingBridgeProtocolEngine *newTestingBridgeProtocolEngine(
    IFileCloser                         &fileCloser,
    const BulkRequestConfigParams       &bulkRequestConfigParams,
    const TapeFlushConfigParams         &tapeFlushConfigParams,
    const Cuuid_t                       &cuuid,
    const int                           rtcpdListenSock,
    const int                           initialRtcpdSock,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    const tapegateway::Volume           &volume,
    const uint32_t                      nbFilesOnDestinationTape,
    utils::BoolFunctor                  &stoppingGracefully,
    Counter<uint64_t>                   &tapebridgeTransactionCounter,
    const bool                          logPeerOfCallbackConnectionsFromRtcpd,
    const bool                          checkRtcpdIsConnectingFromLocalHost,
    IClientProxy                        &clientProxy,
    ILegacyTxRx                         &legacyTxRx)
    throw(std::exception) {
    TestingBridgeProtocolEngine *engine = NULL;

    try {
      engine = new TestingBridgeProtocolEngine(
        fileCloser,
        bulkRequestConfigParams,
        tapeFlushConfigParams,
        cuuid,
        rtcpdListenSock,
        initialRtcpdSock,
        jobRequest,
        volume,
        nbFilesOnDestinationTape,
        stoppingGracefully,
        tapebridgeTransactionCounter,
        logPeerOfCallbackConnectionsFromRtcpd,
        checkRtcpdIsConnectingFromLocalHost,
        clientProxy,
        legacyTxRx);
    } catch(castor::exception::Exception &ce) {
      test_exception te(ce.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception whilst creating an"
        " instance of BridgeProtocolEngine");
    }

    return engine;
  }

  /**
   * This unit-test checks that the BridgeProtocolEngine correctly handles the
   * shutdown logic of the legacy RTCOPY-protocol.
   */
  void testShutdownOfProtocolUsingLocalDomain() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Check the BridgeProtocolEngine can handle the case of no select events
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of no select events",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and create a disk/tape I/O control-connection
    // with the BridgeProtocolEngine
    {
      struct sockaddr_un listenAddr;
      memset(&listenAddr, 0, sizeof(listenAddr));
      listenAddr.sun_family = PF_LOCAL;
      strncpy(listenAddr.sun_path, m_bridgeListenSockPath,
        sizeof(listenAddr.sun_path) - 1);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check creation of disk/tape I/O control-connection",
        0,
        connect(m_ioControlConnectionSock,
          (const struct sockaddr *)&listenAddr, sizeof(listenAddr)));
    }
      
    // Acts as the BridgeProtocolEngine and accept the disk/tape I/O
    // control-connection from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the I/O control-connection connect event",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the new I/O control-connection has been counted",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and write a end of disk/tape I/O
    // control-connection message to the BridgeProtocolEngine
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check writeRTCP_ENDOF_REQ to I/O control-connection",
      unittest::writeRTCP_ENDOF_REQ(m_ioControlConnectionSock));

    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of read of RTCP_ENDOF_REQ message",
        m_engine->handleSelectEvents(selectTimeout));
    }

    // Act as the rtcpd daemon and read in the positive acknowledgement from
    // BridgeProtocolEngine
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check BridgeProtocolEngine has sent positive acknowledge",
      unittest::readAck(m_ioControlConnectionSock, RTCOPY_MAGIC,
        RTCP_ENDOF_REQ, 0));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check BridgeProtocolEngine has closed its end of the I/O"
      " control-connection",
      unittest::connectionHasBeenClosedByPeer(m_ioControlConnectionSock));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the number of I/O control-connection is now 0",
      0,
      m_engine->getNbDiskTapeIOControlConns());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check 1 RTCP_ENDOF_REQ message has been received",
      (uint32_t)1,
      m_engine->getNbReceivedENDOF_REQs());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has finished",
      true,
      m_engine->sessionWithRtcpdIsFinished());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should not continue processing sockets",
      false,
      m_engine->continueProcessingSocks());

    // One socket should have been closed by the BridgeProtocolEngine:
    // * The disk/tape I/O control-connection
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that two sockets have been closed by the BridgeProtocolEngine",
      (std::vector<int>::size_type)1,
      m_fileCloser.m_closedFds.size());
  }


  /**
   * This unit-test checks that the bridge protocol-engine handles a tape being
   * disabled when the bridge protocol-engine requests the first file to be
   * migrated.
   *
   * RTCPD            BRIDGE                         GATE
   *   |                 |                             |
   *   |                 |                             |
   *   |                 |                             |
   *   |                 |         First file?         |
   *   |                 |---------------------------->|
   *   |                 |                             |
   *   |                 |        Tape disabled        |
   *   |                 |<----------------------------|
   *   |                 |                             |
   *   |  RTCP_ENDOF_REQ |                             |
   *   |<----------------|                             |
   *   |                 |                             |
   *   |     Ack         |                             |
   *   |---------------->|                             |
   *   |                 |                             |
   *   |                 | EndNotificationErrorReport  |
   *   |                 |---------------------------->|
   *   |                 |                             |
   *   |                 |   NotificationAcknowledge   |
   *   |                 |<----------------------------|
   *   |                 |                             |
   */
  void testGetFirstFileToMigrateFromDisabledTapeUsingLocalDomain() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Create the thread the "start rtcpd-session" thread
    //
    // The thread will first send a FilesToMigrateListRequest to the client,
    // it will block waiting for the reply
    StartRtcpdSessionThreadParams startRtcpdSessionThreadParams;
    startRtcpdSessionThreadParams.inEngine = m_engine;
    pthread_t startRtcpdSessionThreadId;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_create", 0,
      pthread_create(&startRtcpdSessionThreadId, NULL,
      runStartRtcpdSessionThread, &startRtcpdSessionThreadParams));

    // Push the id of the "start rtcpd-session" thread onto the back of the
    // list of threads to be joined with at tearDown, because an exception may
    // be thrown before this test has a chance to call join itself
    m_threadsToJoinWithAtTearDown.push_back(startRtcpdSessionThreadId);

    // Act as the client and accept the connection for more work from the
    // BridgeProtocolEngine
    int clientConnection1Fd = -1;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check accept of first client-connection from the BridgeProtcolEngine",
       clientConnection1Fd = unittest::netAcceptConnection(
         m_clientListenSock, m_netTimeout));
    castor::io::AbstractTCPSocket clientMarshallingSock1(clientConnection1Fd);
    clientMarshallingSock1.setTimeout(1);

    // Act as the client and read in the first request for more work
    std::auto_ptr<IObject> moreWorkRequestObj;
    tapegateway::FilesToMigrateListRequest *moreWorkRequest = NULL;
    {
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in request for more migration-work from the"
        " BridgeProtocolEngine",
        moreWorkRequestObj.reset(clientMarshallingSock1.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check request for more migration-work from the"
        " BridgeProtocolEngine was read in",
        NULL != moreWorkRequestObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check FilesToMigrateListRequest message is of the correct type",
        (int)castor::OBJ_FilesToMigrateListRequest,
        moreWorkRequestObj->type());
      moreWorkRequest = dynamic_cast<tapegateway::FilesToMigrateListRequest*>
        (moreWorkRequestObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check dynamic_cast to FilesToMigrateListRequest",
        NULL != moreWorkRequest);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check mountTransactionId of request for more work",
        (u_signed64)m_mountTransactionId,
        moreWorkRequest->mountTransactionId());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxFiles of request for more migration-work for first file",
        (u_signed64)1,
        moreWorkRequest->maxFiles());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxBytes of request for more migration-work for first file",
        (u_signed64)1,
        moreWorkRequest->maxBytes());
    }

    // Act as the client and send back a tape DISABLED message
    {
      tapegateway::EndNotificationErrorReport errorReport;

      errorReport.setMountTransactionId(m_mountTransactionId);
      errorReport.setAggregatorTransactionId(
        moreWorkRequest->aggregatorTransactionId());
      errorReport.setErrorCode(ETHELD);
      errorReport.setErrorMessage("castor::tape::tapegateway::"
        "VmgrTapeGatewayHelper::getTapeStatusInVmgr tape is not available:"
        " DISABLED");

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Send tape DISABLED message",
        clientMarshallingSock1.sendObject(errorReport));
      clientMarshallingSock1.close();
    }

    // Act as the rtcpd daemon and recieve the RTCP_ENDOF_REQ message from the
    // initial rtcpd-connection
    {
      legacymsg::MessageHeader rtcpEndOfReqMsg;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Receive header of RTCP_ENDOF_REQ message from BridgeProtocolEngine",
        m_legacyTxRx.receiveMsgHeader(m_initialRtcpdSockRtcpdSide,
        rtcpEndOfReqMsg));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check magic number of RTCP_ENDOF_REQ message from"
        " BridgeProtocolEngine",
        (uint32_t)RTCOPY_MAGIC,
        rtcpEndOfReqMsg.magic);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check request type of RTCP_ENDOF_REQ message from"
        " BridgeProtocolEngine",
        (uint32_t)RTCP_ENDOF_REQ,
        rtcpEndOfReqMsg.reqType);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check staus of RTCP_ENDOF_REQ message from"
        " BridgeProtocolEngine",
        (uint32_t)0,
        rtcpEndOfReqMsg.lenOrStatus);
    }

    // Act as the rtcpd daemon and send back an acknowledge to the
    // BridgeProtocolEngine
    {
      legacymsg::MessageHeader ackMsg;
      ackMsg.magic       = RTCOPY_MAGIC;
      ackMsg.reqType     = RTCP_ENDOF_REQ;
      ackMsg.lenOrStatus = 0;

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Send ack of RTCP_ENDOF_REQ message to BridgeProtocolEngine",
        m_legacyTxRx.sendMsgHeader(m_initialRtcpdSockRtcpdSide, ackMsg));
    }

    // Act as the client and accept the second connection from the
    // BridgeProtocolEngine.  This connection will be used by the
    // BridgeProtocolEngine to send the EndNotificationError report that
    // effective mirrors the EndNotificationError report sent to the
    // BridgeProtocolEngine by the client to report the DISABLED tape.
    int clientConnection2Fd = -1;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check accept of second client-connection from the BridgeProtcolEngine",
      clientConnection2Fd = unittest::netAcceptConnection(
        m_clientListenSock, m_netTimeout));
    castor::io::AbstractTCPSocket clientMarshallingSock2(clientConnection2Fd);
    clientMarshallingSock2.setTimeout(1);

    // Act as the client and read in the mirrored EndNotificationError from
    // the BridgeProtocolEngine.
    {
      std::auto_ptr<IObject> errorReportObj;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in the mirrored EndNotificationErrorReport from the"
        " BridgeProtocolEngine",
        errorReportObj.reset(clientMarshallingSock2.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check the mirrored EndNotificationErrorReport from the"
        " BridgeProtocolEngine was read in",
        NULL != errorReportObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check EndNotificationErrorReport message is of the correct type",
        (int)castor::OBJ_EndNotificationErrorReport,
        errorReportObj->type());
      tapegateway::EndNotificationErrorReport *const errorReport =
        dynamic_cast<tapegateway::EndNotificationErrorReport*>
        (errorReportObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check dynamic_cast to EndNotificationErrorReport",
        NULL != errorReport);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check mountTransactionId of EndNotificationErrorReport",
        (u_signed64)m_mountTransactionId,
        errorReport->mountTransactionId());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check errorCode of EndNotificationErrorReport",
        ETHELD,
        errorReport->errorCode());
    }

    // Join with the start rtcpd session thread
    void *startRtcpdSessionThreadResult = NULL;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("pthread_join", 0,
      pthread_join(startRtcpdSessionThreadId,
      &startRtcpdSessionThreadResult));

    // Remove the id of the "start rtcpd session" thread from the list of
    // threads that should be joined with at tearDown
    m_threadsToJoinWithAtTearDown.remove(startRtcpdSessionThreadId);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Thread results are same structure",
      (void *)&startRtcpdSessionThreadParams, startRtcpdSessionThreadResult);

    if(startRtcpdSessionThreadParams.outAnErrorOccurred) {
      test_exception te(startRtcpdSessionThreadParams.outErrorStream.str());
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "The startRtcpdSessionThread thread encountered an error",
        throw te);
    }
  }

  /**
   * This unit-test checkd that the BridgeProtocolEngine waits for all of its
   * requests to the client for more work to be answered by the client before
   * ending a tape session, even in the event the client reports a DISABLED
   * tape.
   *
   * Here is a description of the following bug where a migration job gets
   * stuck forever because it exists after the end of its parent tape-session:
   *
   *     https://savannah.cern.ch/bugs/index.php?92460
   *     bug #92460: tapebridged should gracefully shutdown a migration
   *     tape-session when tapegatewayd reports a disabled tape
   *
   * Assume that the rtcpd daemon has enough memory to carry out the
   * concurrent recalls to memory of two migration jobs know as J1 and J2.
   *
   * The rtcpd daemon requests more work and gets the first file to migrate.
   * Knowing the size of the file, the rtcpd daemon reservers only the memory
   * needed to migrate that file.  The rtcpd daemon has more memory and
   * therefore requests a second file by sending a second request more work.
   *
   * RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
   *   |                 |                  |               |          |
   *   | J1: more work?  |                  |               |          |
   *   |---------------->|                  |               |          |
   *   |                 | J1: more work?   |               |          |
   *   |                 |----------------->|               |          |
   *   |                 |                  |               |          |
   *   |                 |   J1: work       |               |          |
   *   |                 |<-----------------|               |          |
   *   |   J1: work      |                  |               |          |
   *   |<----------------|                  |               |          |
   *   |                 |                  |               |          |
   *   | J2: more work?  |                  |               |          |
   *   |---------------->|                  |               |          |
   *   |                 | J2: more work?   |               |          |
   *   |                 |----------------->|               |          |
   *   |                 |                  |               |          |
   *
   *  The tapegateway is slow in processing job J2'S request for more work.
   *
   *  In the meantime somebody disables the tape and then the rtcpd daemon
   *  completes job J1.  The VMGR reports the tape is DISABLED when the
   *  tapegateway tries to update the tape.
   *
   * RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
   *   |                 |                  |               |          |
   *   |                 |                  |               |  DISABLE |
   *   |                 |                  |               |<---------|
   *   |                 |   J1: done       |               |          |
   *   |                 |----------------->|               |          |
   *   |                 |                  |  J1: update   |          |
   *   |                 |                  |-------------->|          |
   *   |                 |                  |               |          |
   *   |                 |                  |  J1: DISABLED |          |
   *   |                 |                  |<--------------|          |
   *   |                 |  J1: DISABLED    |               |          |
   *   |                 |<-----------------|               |          |
   *
   * The BridgeProtocolEngine incorrectly responds to the disabled-tape
   * message by immediately telling the tapegateway to end the tape session.
   * The tapebridge should have first waited for the reply to it's request for
   * more work for job J2.
   *
   * RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
   *   |                 |                  |               |          |
   *   |                 | J2: END SESSION  |               |          |
   *   |                 |----------------->|               |          |
   *   |                 |                  |               |          |
   *
   * The tape session is now over (protocol details are not shown).
   *
   * After a delay the tapegateway processes the j2 request for more work
   *
   * RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
   *   |                 |                  |               |          |
   *   |                 |   J2: work       |               |          |
   *   |                 |<-----------------|               |          |
   *
   * Now job j2 is outside of its tape session and is therefore stuck forever.
   */
  void testMigrationToDisabledTapeUsingLocalDomain() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Check the BridgeProtocolEngine can handle the case of no select events
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of no select events",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and create a disk/tape I/O control-connection
    // with the BridgeProtocolEngine
    {
      struct sockaddr_un listenAddr;
      memset(&listenAddr, 0, sizeof(listenAddr));
      listenAddr.sun_family = PF_LOCAL;
      strncpy(listenAddr.sun_path, m_bridgeListenSockPath,
        sizeof(listenAddr.sun_path) - 1);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check creation of disk/tape I/O control-connection",
        0,
        connect(m_ioControlConnectionSock,
          (const struct sockaddr *)&listenAddr, sizeof(listenAddr)));
    }
      
    // Act as the BridgeProtocolEngine and accept the disk/tape I/O
    // control-connection from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the I/O control-connection connect event",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the new I/O control-connection has been counted",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and send a request for more work using the
    // newly created disk/tape I/O control-connection
    const char *const tapePath = "";
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check writeRTCP_REQUEST_MORE_WORK()",
      unittest::writeRTCP_REQUEST_MORE_WORK(
        m_ioControlConnectionSock,
        m_volReqId,
        tapePath));

    // Act as the BridgeProtocolEngine and handle the first
    // RTCP_REQUEST_MORE_WORK message from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first RTCP_REQUEST_MORE_WORK message",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the client and accept the connection for more work from the
    // BridgeProtocolEngine
    int clientConnection1Fd = -1;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check accept of first client-connection from the BridgeProtcolEngine",
       clientConnection1Fd = unittest::netAcceptConnection(
         m_clientListenSock, m_netTimeout));
    castor::io::AbstractTCPSocket clientMarshallingSock1(clientConnection1Fd);
    clientMarshallingSock1.setTimeout(1);

    // Act as the client and read in the first request for more work
    std::auto_ptr<IObject> moreWorkRequestObj;
    tapegateway::FilesToMigrateListRequest *moreWorkRequest = NULL;
    {
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in request for more migration-work from the"
        " BridgeProtocolEngine",
        moreWorkRequestObj.reset(clientMarshallingSock1.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check request for more migration-work from the"
        " BridgeProtocolEngine was read in",
        NULL != moreWorkRequestObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check FilesToMigrateListRequest message is of the correct type",
        (int)castor::OBJ_FilesToMigrateListRequest,
        moreWorkRequestObj->type());
      moreWorkRequest = dynamic_cast<tapegateway::FilesToMigrateListRequest*>
        (moreWorkRequestObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check dynamic_cast to FilesToMigrateListRequest",
        NULL != moreWorkRequest);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check mountTransactionId of request for more work",
        (u_signed64)m_mountTransactionId,
        moreWorkRequest->mountTransactionId());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxFiles of request for more migration-work",
        m_bulkRequestConfigParams.getBulkRequestMigrationMaxFiles().getValue(),
        (uint64_t)moreWorkRequest->maxFiles());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxBytes of request for more migration-work",
        m_bulkRequestConfigParams.getBulkRequestMigrationMaxBytes().getValue(),
        (uint64_t)moreWorkRequest->maxBytes());
    }

    // Act as the client and send back the first file to be migrated
    const char *const nsHostOfFirstFileToMigrate   = "Name-server host";
    const uint64_t    nsFileIdOfFirstFileToMigrate = 2;
    const uint32_t    tapeFSeqOfFirstFileToMigrate = 3;
    const uint64_t    sizeOfFirstFileToMigrate     = 4;
    std::auto_ptr<tapegateway::FileToMigrateStruct> fileToMigrateStruct(
      new tapegateway::FileToMigrateStruct());
    fileToMigrateStruct->setFileTransactionId(1);
    fileToMigrateStruct->setNshost(nsHostOfFirstFileToMigrate);
    fileToMigrateStruct->setFileid(nsFileIdOfFirstFileToMigrate);
    fileToMigrateStruct->setFseq(tapeFSeqOfFirstFileToMigrate);
    fileToMigrateStruct->setPositionCommandCode(tapegateway::TPPOSIT_FSEQ);
    fileToMigrateStruct->setFileSize(sizeOfFirstFileToMigrate);
    fileToMigrateStruct->setLastKnownFilename("Last known filename");
    fileToMigrateStruct->setLastModificationTime(5);
    fileToMigrateStruct->setPath("path");
    fileToMigrateStruct->setUmask(6);
    tapegateway::FilesToMigrateList filesToMigrateList;
    filesToMigrateList.setMountTransactionId(m_mountTransactionId);
    filesToMigrateList.setAggregatorTransactionId(
      moreWorkRequest->aggregatorTransactionId());
    filesToMigrateList.addFilesToMigrate(fileToMigrateStruct.release());
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Send first file to migrate",
      clientMarshallingSock1.sendObject(filesToMigrateList));
    clientMarshallingSock1.close();

    // Create a thread to act as the rtcpd daemon which should read in,
    // acknowledge and then drop the following 3 RTCOPY messages that will
    // soon be sent by the BridgeProtocolEngine:
    //
    //   1. The file to be migrated message
    //   2. The request more work message
    //   3. The end of file-list message
    pthread_t localRtcpdThread;
    memset(&localRtcpdThread, '\0', sizeof(localRtcpdThread));
    const pthread_attr_t *const localRtcpdThreadAttr = NULL;
    std::pair<int, int> ioControlConnectionSockFdAndNMsgs(
      m_ioControlConnectionSock, 3);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Create local rtcpd thread to read and drop 3 messages",
      0,
      pthread_create(&localRtcpdThread, localRtcpdThreadAttr,
        unittest::readInAckAndDropNRtcopyMsgs,
        (void *)&ioControlConnectionSockFdAndNMsgs));

    // Push the id of the "local rtcpd" thread onto the back of the
    // list of threads to be joined with at tearDown, because an exception may
    // be thrown before this test has a chance to call join itself
    m_threadsToJoinWithAtTearDown.push_back(localRtcpdThread);

    // Act as the BridgeProtocolEngine and handle the first file to be
    // migrated from the client
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file to be migrated from the client",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Join with the rtcpd thread
    void *localRtcpdThreadVoidResult = NULL; 
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Join with local rtcpd thread that read and dropped 3 messages",
      0,
      pthread_join(localRtcpdThread, &localRtcpdThreadVoidResult));

    // Remove the id of the "local rtcpd" thread from the list of
    // threads that should be joined with at tearDown
    m_threadsToJoinWithAtTearDown.remove(localRtcpdThread);

    // Act as the rtcpd daemon and read in from the disk/tape-I/O
    // control-connection socket the delayed ACK of the request for more work
    {
       char ackBuf[12];

       CPPUNIT_ASSERT_EQUAL_MESSAGE(
         "Read in delayed ACK of the request for more work",
         (ssize_t)sizeof(ackBuf),
         read(m_ioControlConnectionSock, ackBuf, sizeof(ackBuf)));
    }

    // Act as the rtcpd daemon and send a transfer-completed message using
    // the disk/tape-I/O control-connection with the BridgeProtocolEngine
    {
      const int32_t     positionMethod = tapegateway::TPPOSIT_FSEQ;
      const uint32_t    diskFseq       = 0;
      const uint64_t    bytesIn        = sizeOfFirstFileToMigrate;
      const uint64_t    bytesOut       = sizeOfFirstFileToMigrate;
      struct legacymsg::RtcpSegmentAttributes segAttr;
      memset(&segAttr, '\0', sizeof(segAttr));
      strncpy(segAttr.nameServerHostName, nsHostOfFirstFileToMigrate,
        sizeof(segAttr.nameServerHostName) - 1);
      strncpy(segAttr.segmCksumAlgorithm, "alder32",
        sizeof(segAttr.segmCksumAlgorithm) - 1);
      segAttr.segmCksum = 0xDEADFACE;
      segAttr.castorFileId = nsFileIdOfFirstFileToMigrate;

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check writeRTCP_FINISHED()",
        unittest::writeRTCP_FINISHED(
          m_ioControlConnectionSock,
          m_volReqId,
          tapePath,
          positionMethod,
          tapeFSeqOfFirstFileToMigrate,
          diskFseq,
          bytesIn,
          bytesOut,
          segAttr));
    }

    // Act as the BridgeProtocolEngine and handle the transfer-completed
    // message from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file being migrated",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and read back the ACK from the
    // BridgeProtocolEngine
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check read back of ACK of RTCP_FINISHED message",
      unittest::readAck(m_ioControlConnectionSock, RTCOPY_MAGIC,
        RTCP_FILEERR_REQ, 0));

    // Act as the rtcpd daemon and send the BridgeProtocolEngine a
    // flushed-to-tape message
    {
      tapeBridgeFlushedToTapeMsgBody_t flushedMsgBody;
      memset(&flushedMsgBody, '\0', sizeof(flushedMsgBody));
      flushedMsgBody.volReqId = m_volReqId;
      flushedMsgBody.tapeFseq = tapeFSeqOfFirstFileToMigrate;

      CPPUNIT_ASSERT_MESSAGE(
        "Check tapebridge_sendTapeBridgeFlushedToTape()",
        0 < tapebridge_sendTapeBridgeFlushedToTape(
          m_ioControlConnectionSock, m_netTimeout, &flushedMsgBody));
    }

    // Act as the BridgeProtocolEngine and handle the flushed-to-tape message
    // sent by the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first flushed-to-tape message",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and read in the ACK from the
    // BridgeProtocolEngine of the flushed-to-tape message
    {
      tapeBridgeFlushedToTapeAckMsg_t flushedAckMsg;
      memset(&flushedAckMsg, '\0', sizeof(flushedAckMsg));

      CPPUNIT_ASSERT_MESSAGE(
        "Check tapebridge_recvTapeBridgeFlushedToTapeAck()",
        0 <= tapebridge_recvTapeBridgeFlushedToTapeAck(
          m_ioControlConnectionSock, m_netTimeout, &flushedAckMsg));
    }

    // Act as the client and accept the second connection from the
    // BridgeProtocolEngine.  This connection will be used by the
    // BridgeProtocolEngine to send the FileMigrationReportList message of
    // the first migrated file.
    int clientConnection2Fd = -1;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check accept of second client-connection from the BridgeProtcolEngine",
      clientConnection2Fd = unittest::netAcceptConnection(
        m_clientListenSock, m_netTimeout));
    castor::io::AbstractTCPSocket clientMarshallingSock2(clientConnection2Fd);
    clientMarshallingSock2.setTimeout(1);

    // The client is now slow to process the second connection from the
    // BridgeProtocolEngine so does nothing at this very moment in time.

    // Act as the rtcpd daemon and send a request for more work using the
    // the already existant disk/tape I/O control-connection
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check writeRTCP_REQUEST_MORE_WORK()",
      unittest::writeRTCP_REQUEST_MORE_WORK(
        m_ioControlConnectionSock,
        m_volReqId,
        tapePath));

    // Act as the BridgeProtocolEngine and handle the second
    // RTCP_REQUEST_MORE_WORK message from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the second RTCP_REQUEST_MORE_WORK message",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // The client now catches up

    // Act as the client and read in the FileMigrationReportList message of
    // the first file to be migrated from the seond connection made with the
    // client by the BridgeProtocolEngine
    std::auto_ptr<IObject> fileMigrationReportListObj;
    tapegateway::FileMigrationReportList *fileMigrationReportList = NULL;
    {
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in FileMigrationReportList the BridgeProtocolEngine",
        fileMigrationReportListObj.reset(
          clientMarshallingSock2.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check FileMigrationReportList was read in from the"
        " BridgeProtocolEngine",
        NULL != fileMigrationReportListObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check FileMigrationReportList is of the correct type",
        (int)castor::OBJ_FileMigrationReportList,
        fileMigrationReportListObj->type());
      fileMigrationReportList =
        dynamic_cast<tapegateway::FileMigrationReportList*>
        (fileMigrationReportListObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check dynamic_cast to FileMigrationReportList",
        NULL != fileMigrationReportList);
    }

    // Act as the client and reply to the BridgeProtocolEngine with an error
    // report
    {
      tapegateway::EndNotificationErrorReport errorReport;
      errorReport.setMountTransactionId(m_mountTransactionId);
      errorReport.setAggregatorTransactionId(
        fileMigrationReportList->aggregatorTransactionId());
      errorReport.setErrorCode(ECANCELED);
      errorReport.setErrorMessage("Error message");

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Send error report in response to first file migrated",
        clientMarshallingSock2.sendObject(errorReport));

      clientMarshallingSock2.close();
    }

    // Act as the BridgeProtocolEngine and handle the error report from the
    // client about the first migrated-file
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file's error report from the client",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is being gracefully shutdown",
      true,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());
  }

  /**
   * This unit-test checks that the BridgeProtocolEngine starts the graceful
   * shutdown of the tape-session when it receives an error in response to a
   * request for the next file to recall.
   *
   * RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
   *   |                 |                  |               |          |
   *   |   more work?    |                  |               |          |
   *   |---------------->|                  |               |          |
   *   |                 |   more work?     |               |          |
   *   |                 |----------------->|               |          |
   *   |                 |                  |               |          |
   *   |                 |       ERROR      |               |          |
   *   |                 |<-----------------|               |          |
   *   |     no more     |                  |               |          |
   *   |<----------------|                  |               |          |
   *   |                 |                  |               |          |
   */
  void testNoMoreRecallsDueToErrorFromTapegatewayUsingLocalDomain() {
    // Create the BridgeProtocolEngine for a recall session
    m_volume.setMode(tapegateway::READ);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Check the BridgeProtocolEngine can handle the case of no select events
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of no select events",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and create a disk/tape I/O control-connection
    // with the BridgeProtocolEngine
    {
      struct sockaddr_un listenAddr;
      memset(&listenAddr, 0, sizeof(listenAddr));
      listenAddr.sun_family = PF_LOCAL;
      strncpy(listenAddr.sun_path, m_bridgeListenSockPath,
        sizeof(listenAddr.sun_path) - 1);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check creation of disk/tape I/O control-connection",
        0,
        connect(m_ioControlConnectionSock,
          (const struct sockaddr *)&listenAddr, sizeof(listenAddr)));
    }
      
    // Act as the BridgeProtocolEngine and accept the disk/tape I/O
    // control-connection from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the I/O control-connection connect event",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the new I/O control-connection has been counted",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the rtcpd daemon and send a request for more work using the
    // newly created disk/tape I/O control-connection
    const char *const tapePath = "";
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check writeRTCP_REQUEST_MORE_WORK()",
      unittest::writeRTCP_REQUEST_MORE_WORK(
        m_ioControlConnectionSock,
        m_volReqId,
        tapePath));

    // Act as the BridgeProtocolEngine and handle the first
    // RTCP_REQUEST_MORE_WORK message from the rtcpd daemon
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first RTCP_REQUEST_MORE_WORK message",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Act as the client and accept the connection for more work from the
    // BridgeProtocolEngine
    int clientConnection1Fd = -1;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check accept of first client-connection from the BridgeProtcolEngine",
       clientConnection1Fd = unittest::netAcceptConnection(
         m_clientListenSock, m_netTimeout));
    castor::io::AbstractTCPSocket clientMarshallingSock1(clientConnection1Fd);
    clientMarshallingSock1.setTimeout(1);

    // Act as the client and read in the first request for more work
    std::auto_ptr<IObject> moreWorkRequestObj;
    tapegateway::FilesToRecallListRequest *moreWorkRequest = NULL;
    {
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in request for more recall-work from the"
        " BridgeProtocolEngine",
        moreWorkRequestObj.reset(clientMarshallingSock1.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check request for more recall-work from the"
        " BridgeProtocolEngine was read in",
        NULL != moreWorkRequestObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check FilesToRecallListRequest message is of the correct type",
        (int)castor::OBJ_FilesToRecallListRequest,
        moreWorkRequestObj->type());
      moreWorkRequest = dynamic_cast<tapegateway::FilesToRecallListRequest*>
        (moreWorkRequestObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check dynamic_cast to FilesToRecallListRequest",
        NULL != moreWorkRequest);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check mountTransactionId of request for more work",
        (u_signed64)m_mountTransactionId,
        moreWorkRequest->mountTransactionId());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxFiles of request for more recall-work",
        m_bulkRequestConfigParams.getBulkRequestRecallMaxFiles().getValue(),
        (uint64_t)moreWorkRequest->maxFiles());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxBytes of request for more migration-work",
        m_bulkRequestConfigParams.getBulkRequestRecallMaxBytes().getValue(),
        (uint64_t)moreWorkRequest->maxBytes());
    }

    // Act as the client and send back an error
    {
      tapegateway::EndNotificationErrorReport errorReport;
      errorReport.setMountTransactionId(m_mountTransactionId);
      errorReport.setAggregatorTransactionId(
        moreWorkRequest->aggregatorTransactionId());
      errorReport.setErrorCode(ECANCELED);
      errorReport.setErrorMessage("Error message");

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Send error report in response to first file recall",
        clientMarshallingSock1.sendObject(errorReport));

      clientMarshallingSock1.close();
    }

    // Create a thread to act as the rtcpd daemon and handle the RTCOPY
    // messages from the BridgeProtocolEngine that represent "no more work"
    pthread_t receiveNoMoreWorkFromBridgeThread;
    memset(&receiveNoMoreWorkFromBridgeThread, '\0',
      sizeof(receiveNoMoreWorkFromBridgeThread));
    ReceiveNoMoreWorkFromBridgeThreadParams
      receiveNoMoreWorkFromBridgeThreadParams;
    receiveNoMoreWorkFromBridgeThreadParams.inIoControlConnectionSock =
      m_ioControlConnectionSock;
    receiveNoMoreWorkFromBridgeThreadParams.inLegacyTxRx =
      &m_legacyTxRx;
    {
        const pthread_attr_t *const threadAttr = NULL;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Create rtcpd thread to handle no more work messages from bridge",
        0,
        pthread_create(&receiveNoMoreWorkFromBridgeThread, threadAttr,
        receiveNoMoreWorkFromBridge,
        (void *)&receiveNoMoreWorkFromBridgeThreadParams));
    }

    // Push the id of the "receive no-more work" thread onto the back of the
    // list of threads to be joined with at tearDown, because an exception may
    // be thrown before this test has had a chance to call join
    m_threadsToJoinWithAtTearDown.push_back(receiveNoMoreWorkFromBridgeThread);

    // Act as the BridgeProtocolEngine and handle the error message from the
    // client
    {
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the error message from the client",
        m_engine->handleSelectEvents(selectTimeout));
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the there is still only one I/O control-connection",
      1,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is being shutdown gracefully",
      true,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    // Join with the "receive no-more work" thread
    void *receiveNoMoreWorkFromBridgeThreadResult = NULL; 
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Join with local rtcpd thread that read and dropped 3 messages",
      0,
      pthread_join(receiveNoMoreWorkFromBridgeThread,
        &receiveNoMoreWorkFromBridgeThreadResult));

    // Remove the id of the "receive no-more work" thread from the list of
    // threads that should be joined with at tearDown
    m_threadsToJoinWithAtTearDown.remove(receiveNoMoreWorkFromBridgeThread);

    // Check the result of the "receive no-more work" thread
    if(receiveNoMoreWorkFromBridgeThreadParams.outAnErrorOccurred) {
      test_exception te(
        receiveNoMoreWorkFromBridgeThreadParams.outErrorStream.str());
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "The receive no-more work thread encountered an error",
        throw te);
    }  
  }


  /**
   * This unit-test checks that the BridgeProtocolEngine correctly generates
   * the hexadecimal tape-file identifiers used by the legacy RTCOPY-protocol.
   */
  void testGenerateMigrationTapeFileId() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check there are no I/O control-connections",
      0,
      m_engine->getNbDiskTapeIOControlConns());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check no RTCP_ENDOF_REQ messages have been received",
      (uint32_t)0,
      m_engine->getNbReceivedENDOF_REQs());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check the session with the rtcpd daemon is not being shutdown",
      false,
      m_engine->shuttingDownRtcpdSession());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check rtcpd session has not finished",
      false,
      m_engine->sessionWithRtcpdIsFinished());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check BridgeProtocolEngine should continue processing sockets",
      true,
      m_engine->continueProcessingSocks());

    const uint64_t i               = 0xdeadfacedeadface;
    const char     *expectedResult =  "DEADFACEDEADFACE";
    char dst[CA_MAXPATHLEN+1];
    memset(dst, '\0', sizeof(dst));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Checking engine.generateMigrationTapeFileId()",
      m_engine->generateMigrationTapeFileId(i, dst));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check result of engine.generateMigrationTapeFileId()",
      std::string(expectedResult),
      std::string(dst));
  }

  /**
   * A good day scenario where the compressed files are half the size of the
   * orginal files.
   *
   * Please note that each file is reported at the time of writing to be a
   * quarter of the size of the original and the flush at the end the writing
   * the batch writes one quarter of the total of the original file sizes.
   */
  void testCalcMigrationCompressionRatio() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    const unsigned int nbFiles = 10;
    const uint64_t fileSize = 1024;
    FileWrittenNotificationList migrations;

    for(uint64_t i=1; i<=nbFiles; i++) {
      FileWrittenNotification notification;

      notification.fileSize           = fileSize;
      notification.compressedFileSize = (uint64_t)(fileSize * 0.25);
      migrations.push_back(notification);
    }

    const uint64_t bytesWrittenToTapeByFlush =
      (uint64_t)(nbFiles * fileSize * (double)0.25);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking compression ratio",
      (double)0.5,
      m_engine->calcMigrationCompressionRatio(migrations,
        bytesWrittenToTapeByFlush));
  }

  /**
   * A good day scenario test of the method named
   * BridgeProtocolEngine::correctMigrationCompressionStatistics().
   *
   * The overal compression ratio is 0.5, that is to say the tape writes half
   * as much data to tape than it receives.  However due to the asynchronous
   * reading out of the bytes written to tape, the drive only reports that a
   * quarter of each file has been written when the drive is queried, and only
   * the final flush does the rest of the data get reported.
   */
  void testCorrectMigrationCompressionStatistics() {
    // Create the BridgeProtocolEngine for a migration session
    m_volume.setMode(tapegateway::WRITE);
    const bool logPeerOfCallbackConnectionsFromRtcpd = false;
    const bool checkRtcpdIsConnectingFromLocalHost = false;
    m_engine = newTestingBridgeProtocolEngine(
      m_fileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      m_cuuid,
      m_bridgeListenSock,
      m_initialRtcpdSockBridgeSide,
      m_jobRequest,
      m_volume,
      m_nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      m_clientProxy,
      m_legacyTxRx);

    const unsigned int nbFiles = 10;
    const uint64_t fileSize = 1024;
    FileWrittenNotificationList migrations;
    const uint64_t bytesWrittenToTapeByFlush = nbFiles * fileSize / 4;

    for(uint64_t i=1; i<=nbFiles; i++) {
      FileWrittenNotification notification;

      notification.fileTransactionId  = i;
      notification.nsFileId           = i * 2;
      notification.fileSize           = fileSize;
      notification.compressedFileSize = fileSize / 4;
      migrations.push_back(notification);
    }

    m_engine->correctMigrationCompressionStatistics(migrations,
      bytesWrittenToTapeByFlush);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Checking size of notification list did not change",
      nbFiles,
      (unsigned int)migrations.size());

    {
      uint64_t i = 1;
      for(
        FileWrittenNotificationList::const_iterator itor = migrations.begin();
        itor != migrations.end();
        itor++, i++) {
        const FileWrittenNotification &notification = *itor;

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Checking fileTransactionId",
          i,
          notification.fileTransactionId);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Checking nsFileId",
          i * 2,
          notification.nsFileId);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Checking fileSize",
          fileSize,
          notification.fileSize);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Checking compressedFileSize",
          fileSize / 2,
          notification.compressedFileSize);
      }
    }
  }

  CPPUNIT_TEST_SUITE(BridgeProtocolEngineTest);

  CPPUNIT_TEST(testShutdownOfProtocolUsingLocalDomain);
  CPPUNIT_TEST(testGetFirstFileToMigrateFromDisabledTapeUsingLocalDomain);
  CPPUNIT_TEST(testMigrationToDisabledTapeUsingLocalDomain);
  CPPUNIT_TEST(testNoMoreRecallsDueToErrorFromTapegatewayUsingLocalDomain);
  CPPUNIT_TEST(testGenerateMigrationTapeFileId);
  CPPUNIT_TEST(testCalcMigrationCompressionRatio);
  CPPUNIT_TEST(testCorrectMigrationCompressionStatistics);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeProtocolEngineTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
