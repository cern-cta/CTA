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
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/ClientProxy.hpp"
#include "castor/tape/tapebridge/ClientAddressLocal.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
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

public:

  BridgeProtocolEngineTest():
    m_clientListenSockPath("/tmp/clientListenSockForBridgeProtocolEngineTest"),
    m_bridgeListenSockPath("/tmp/brigdeListenSockForBridgeProtocolEngineTest") {
    // Do nothing
  }

  void setUp() {
    unlink(m_bridgeListenSockPath);
  }

  void tearDown() {
    unlink(m_bridgeListenSockPath);
  }

  void testConstructor() {
    TraceableDummyFileCloser fileCloser;
    const int initialRtcpdSock = 12;

    {
      TestingBulkRequestConfigParams      bulkRequestConfigParams;
      TestingTapeFlushConfigParams        tapeFlushConfigParams;
      const Cuuid_t                       &cuuid = nullCuuid;
      const int                           rtcpdListenSock  = 11;
      const uint32_t                      mountTransactionId = 12;
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
      const tapegateway::Volume volume;
      const uint32_t            nbFilesOnDestinationTape = 0;
      AlwaysFalseBoolFunctor    stoppingGracefully;
      Counter<uint64_t>         tapebridgeTransactionCounter(0);
      const bool                logPeerOfCallbackConnectionsFromRtcpd = false;
      const bool                checkRtcpdIsConnectingFromLocalHost = false;
      DummyClientProxy          clientProxy;
      std::auto_ptr<TestingBridgeProtocolEngine> smartEngine;

      bulkRequestConfigParams.setBulkRequestMigrationMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestMigrationMaxFiles(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxFiles(1,
        ConfigParamSource::UNDEFINED);

      tapeFlushConfigParams.setTapeFlushMode(TAPEBRIDGE_N_FLUSHES_PER_FILE,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxBytesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxFilesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newTestingBridgeProtocolEngine()",
        smartEngine.reset(newTestingBridgeProtocolEngine(
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
          clientProxy)));
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that only one socket has been closed",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that initialRtcpdSock was the socket that was closed",
      initialRtcpdSock,
      fileCloser.m_closedFds.front());
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
    IClientProxy                        &clientProxy)
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
        clientProxy);
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

  void testShutdownOfProtocolUsingLocalDomain() {
    // Create the socket on which the client would listen for incoming
    // connections from the BridgeProtocolEngine
    utils::SmartFd smartClientListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local client listen socket",
       smartClientListenSock.reset(
        unittest::createLocalListenSocket(m_clientListenSockPath)));

    // Create the initial rtcpd-connection using socketpair()
    int initialRtcpdSockPair[2] = {-1, -1};
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check socket pair for the intital rtcpd-connection has been created",
      0,
      socketpair(PF_LOCAL, SOCK_STREAM, 0, initialRtcpdSockPair));
    utils::SmartFd smartInitialRtcpdSockRtcpdSide(initialRtcpdSockPair[0]);
    utils::SmartFd smartInitialRtcpdSockBridgeSide(initialRtcpdSockPair[1]);
    const int initialRtcpdSockBridgeSide =
      smartInitialRtcpdSockBridgeSide.get();

    // Create the socket on which the BridgeProtocolEngine will listen for
    // incoming connections from the rtcpd daemon
    utils::SmartFd smartBridgeListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local rtcpd listen socket",
       smartBridgeListenSock.reset(
        unittest::createLocalListenSocket(m_bridgeListenSockPath)));

    // Create a socket for the disk/tape I/O control-connection
    utils::SmartFd
      smartIoControlConnectionSock(socket(PF_LOCAL, SOCK_STREAM, 0));
    CPPUNIT_ASSERT_MESSAGE(
      "Create a socket for the first disk/tape I/O control-connection",
      -1 != smartIoControlConnectionSock.get());

    TraceableSystemFileCloser fileCloser;
    {
      TestingBulkRequestConfigParams      bulkRequestConfigParams;
      TestingTapeFlushConfigParams        tapeFlushConfigParams;
      const Cuuid_t                       &cuuid = nullCuuid;
      const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        0, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };

      tapegateway::Volume        volume;
      const uint32_t             nbFilesOnDestinationTape = 2;
      AlwaysFalseBoolFunctor     stoppingGracefully;
      Counter<uint64_t>          tapebridgeTransactionCounter(0);
      const bool                 logPeerOfCallbackConnectionsFromRtcpd = false;
      const bool                 checkRtcpdIsConnectingFromLocalHost = false;
      const uint32_t             mountTransactionId = 1;
      const int                  netTimeout = 1;
      ClientAddressLocal         clientAddress(m_clientListenSockPath);
      const std::string          dgn("dgn");
      const std::string          driveUnit("unit");
      std::auto_ptr<ClientProxy> smartClientProxy;
      std::auto_ptr<TestingBridgeProtocolEngine> smartEngine;

      bulkRequestConfigParams.setBulkRequestMigrationMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestMigrationMaxFiles(2,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxBytes(3,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxFiles(4,
        ConfigParamSource::UNDEFINED);

      tapeFlushConfigParams.setTapeFlushMode(TAPEBRIDGE_N_FLUSHES_PER_FILE,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxBytesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxFilesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);

      // Create a client proxy
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newClientProxy()",
        smartClientProxy.reset(newClientProxy(
          cuuid,
          mountTransactionId,
          netTimeout,
          clientAddress,
          dgn,
          driveUnit)));

      // Create a BridgeProtocolEngine
      volume.setVid("vid");
      volume.setDensity("density");
      volume.setLabel("label");
      volume.setMode(tapegateway::WRITE); // Test migration-logic
      volume.setClientType(tapegateway::TAPE_GATEWAY);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newTestingBridgeProtocolEngine()",
        smartEngine.reset(newTestingBridgeProtocolEngine(
          fileCloser,
          bulkRequestConfigParams,
          tapeFlushConfigParams,
          cuuid,
          smartBridgeListenSock.get(),
          initialRtcpdSockBridgeSide,
          jobRequest,
          volume,
          nbFilesOnDestinationTape,
          stoppingGracefully,
          tapebridgeTransactionCounter,
          logPeerOfCallbackConnectionsFromRtcpd,
          checkRtcpdIsConnectingFromLocalHost,
          *smartClientProxy.get())));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check there are no I/O control-connections",
        0,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Check the BridgeProtocolEngine can handle the case of no select events
      {
        struct timeval selectTimeout = {0, 0};
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "Check handling of no select events",
          smartEngine->handleSelectEvents(selectTimeout));
      }
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check there are no I/O control-connections",
        0,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

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
          connect(smartIoControlConnectionSock.get(),
            (const struct sockaddr *)&listenAddr, sizeof(listenAddr)));
      }
        
      // Acts as the BridgeProtocolEngine and accept the disk/tape I/O
      // control-connection from the rtcpd daemon
      {
        struct timeval selectTimeout = {0, 0};
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "Check handling of the I/O control-connection connect event",
          smartEngine->handleSelectEvents(selectTimeout));
      }
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the new I/O control-connection has been counted",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Act as the rtcpd daemon and write a end of disk/tape I/O
      // control-connection message to the BridgeProtocolEngine
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check writeRTCP_ENDOF_REQ to I/O control-connection",
        unittest::writeRTCP_ENDOF_REQ(smartIoControlConnectionSock.get()));

      {
        struct timeval selectTimeout = {0, 0};
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "Check handling of read of RTCP_ENDOF_REQ message",
          smartEngine->handleSelectEvents(selectTimeout));
      }

      // Act as the rtcpd daemon and read in the positive acknowledgement from
      // BridgeProtocolEngine
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check BridgeProtocolEngine has sent positive acknowledge",
        unittest::readAck(smartIoControlConnectionSock.get(), RTCOPY_MAGIC,
          RTCP_ENDOF_REQ, 0));

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check BridgeProtocolEngine has closed its end of the I/O"
        " control-connection",
        unittest::connectionHasBeenClosedByPeer(
          smartIoControlConnectionSock.get()));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the number of I/O control-connection is now 0",
        0,
        smartEngine->getNbDiskTapeIOControlConns());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check 1 RTCP_ENDOF_REQ message has been received",
        (uint32_t)1,
        smartEngine->getNbReceivedENDOF_REQs());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has finished",
        true,
        smartEngine->sessionWithRtcpdIsFinished());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should not continue processing sockets",
        false,
        smartEngine->continueProcessingSocks());
    }

    // Two sockets should have been closed by the BridgeProtocolEngine:
    // * The initial connection from rtcpd
    // * The disk/tape I/O control-connection
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that two sockets have been closed by the BridgeProtocolEngine",
      (std::vector<int>::size_type)2,
      fileCloser.m_closedFds.size());

    CPPUNIT_ASSERT_MESSAGE(
      "Check that the initialRtcpdSock was one of the two closed-sockets",
      fileCloser.m_closedFds[0] == initialRtcpdSockBridgeSide ||
      fileCloser.m_closedFds[1] == initialRtcpdSockBridgeSide);
  }

  // The BridgeProtocolEngine must wait for all of its requests to the client
  // for more work to be answered by the clinet before ending a tape session,
  // even in the event the client reports a DISABLED tape.
  //
  // Here is a description of the following bug where a migration job gets
  // stuck forever because it exists after the end of its parent tape-session:
  //
  //     https://savannah.cern.ch/bugs/index.php?92460
  //     bug #92460: tapebridged should gracefully shutdown a migration
  //     tape-session when tapegatewayd reports a disabled tape
  //
  // Assume that the rtcpd daemon has enough memory to carry out the
  // concurrent recalls to memory of two migration jobs know as J1 and J2.
  //
  // The rtcpd daemon requests more work and gets the first file to migrate.
  // Knowing the size of the file, the rtcpd daemon reservers only the memory
  // needed to migrate that file.  The rtcpd daemon has more memory and
  // therefore requests a second file by sending a second request more work.
  //
  // RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
  //   |                 |                  |               |          |
  //   | J1: more work?  |                  |               |          |
  //   |---------------->|                  |               |          |
  //   |                 | J1: more work?   |               |          |
  //   |                 |----------------->|               |          |
  //   |                 |                  |               |          |
  //   |                 |   J1: work       |               |          |
  //   |                 |<-----------------|               |          |
  //   |   J1: work      |                  |               |          |
  //   |<----------------|                  |               |          |
  //   |                 |                  |               |          |
  //   | J2: more work?  |                  |               |          |
  //   |---------------->|                  |               |          |
  //   |                 | J2: more work?   |               |          |
  //   |                 |----------------->|               |          |
  //   |                 |                  |               |          |
  //
  //  The tapegateway is slow in processing job J2'S request for more work.
  //
  //  In the meantime somebody disables the tape and then the rtcpd daemon
  //  completes job J1.  The VMGR reports the tape is DISABLED when the
  //  tapegateway tries to update the tape.
  //
  // RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
  //   |                 |                  |               |          |
  //   |                 |                  |               |  DISABLE |
  //   |                 |                  |               |<---------|
  //   |                 |   J1: done       |               |          |
  //   |                 |----------------->|               |          |
  //   |                 |                  |  J1: update   |          |
  //   |                 |                  |-------------->|          |
  //   |                 |                  |               |          |
  //   |                 |                  |  J1: DISABLED |          |
  //   |                 |                  |<--------------|          |
  //   |                 |  J1: DISABLED    |               |          |
  //   |                 |<-----------------|               |          |
  //
  // The BridgeProtocolEngine incorrectly responds to the disabled-tape
  // message by immediately telling the tapegateway to end the tape session.
  // The tapebridge should have first waited for the reply to it's request for
  // more work for job J2.
  //
  // RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
  //   |                 |                  |               |          |
  //   |                 | J2: END SESSION  |               |          |
  //   |                 |----------------->|               |          |
  //   |                 |                  |               |          |
  //
  // The tape session is now over (protocol details are not shown).
  //
  // After a delay the tapegateway processes the j2 request for more work
  //
  // RTCPD            BRIDGE              GATE            VMGR     SOMEBODY
  //   |                 |                  |               |          |
  //   |                 |   J2: work       |               |          |
  //   |                 |<-----------------|               |          |
  //
  // Now job j2 is outside of its tape session and is therefore stuck forever.
  //
  void testMigrationToDisabledTapeUsingLocalDomain() {
    // Create the socket on which the client would listen for incoming
    // connections from the BridgeProtocolEngine
    utils::SmartFd smartClientListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local client listen socket",
       smartClientListenSock.reset(
        unittest::createLocalListenSocket(m_clientListenSockPath)));

    // Create the initial rtcpd-connection using socketpair()
    int initialRtcpdSockPair[2] = {-1, -1};
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check socket pair for the intital rtcpd-connection has been created",
      0,
      socketpair(PF_LOCAL, SOCK_STREAM, 0, initialRtcpdSockPair));
    utils::SmartFd smartInitialRtcpdSockRtcpdSide(initialRtcpdSockPair[0]);
    utils::SmartFd smartInitialRtcpdSockBridgeSide(initialRtcpdSockPair[1]);
    const int initialRtcpdSockBridgeSide =
      smartInitialRtcpdSockBridgeSide.get();

    // Create the socket on which the BridgeProtocolEngine will listen for
    // incoming connections from the rtcpd daemon
    utils::SmartFd smartBridgeListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local rtcpd listen socket",
       smartBridgeListenSock.reset(
        unittest::createLocalListenSocket(m_bridgeListenSockPath)));

    // Create a socket for the disk/tape I/O control-connection
    utils::SmartFd
      smartIoControlConnectionSock(socket(PF_LOCAL, SOCK_STREAM, 0));
    CPPUNIT_ASSERT_MESSAGE(
      "Create a socket for the first disk/tape I/O control-connection",
      -1 != smartIoControlConnectionSock.get());

    TraceableSystemFileCloser fileCloser;
    {
      TestingBulkRequestConfigParams      bulkRequestConfigParams;
      TestingTapeFlushConfigParams        tapeFlushConfigParams;
      const Cuuid_t                       &cuuid = nullCuuid;
      const uint32_t                      volReqId = 5678;
      const char *const                   tapePath = "";
      const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        volReqId, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };
      tapegateway::Volume        volume;
      const uint32_t             nbFilesOnDestinationTape = 2;
      AlwaysFalseBoolFunctor     stoppingGracefully;
      Counter<uint64_t>          tapebridgeTransactionCounter(0);
      const bool                 logPeerOfCallbackConnectionsFromRtcpd = false;
      const bool                 checkRtcpdIsConnectingFromLocalHost = false;
      const uint32_t             mountTransactionId = 1;
      const int                  netTimeout = 1;
      ClientAddressLocal         clientAddress(m_clientListenSockPath);
      const std::string          dgn("dgn");
      const std::string          driveUnit("unit");
      std::auto_ptr<ClientProxy> smartClientProxy;
      std::auto_ptr<TestingBridgeProtocolEngine> smartEngine;

      bulkRequestConfigParams.setBulkRequestMigrationMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestMigrationMaxFiles(2,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxBytes(3,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxFiles(4,
        ConfigParamSource::UNDEFINED);

      tapeFlushConfigParams.setTapeFlushMode(TAPEBRIDGE_N_FLUSHES_PER_FILE,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxBytesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxFilesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);

      // Create a client proxy
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newClientProxy()",
        smartClientProxy.reset(newClientProxy(
          cuuid,
          mountTransactionId,
          netTimeout,
          clientAddress,
          dgn,
          driveUnit)));

      // Create a BridgeProtocolEngine
      volume.setVid("vid");
      volume.setDensity("density");
      volume.setLabel("label");
      volume.setMode(tapegateway::WRITE); // Test migration-logic
      volume.setClientType(tapegateway::TAPE_GATEWAY);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newTestingBridgeProtocolEngine()",
        smartEngine.reset(newTestingBridgeProtocolEngine(
          fileCloser,
          bulkRequestConfigParams,
          tapeFlushConfigParams,
          cuuid,
          smartBridgeListenSock.get(),
          initialRtcpdSockBridgeSide,
          jobRequest,
          volume,
          nbFilesOnDestinationTape,
          stoppingGracefully,
          tapebridgeTransactionCounter,
          logPeerOfCallbackConnectionsFromRtcpd,
          checkRtcpdIsConnectingFromLocalHost,
          *smartClientProxy.get())));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check there are no I/O control-connections",
        0,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Check the BridgeProtocolEngine can handle the case of no select events
      struct timeval selectTimeout = {0, 0};
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of no select events",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check there are no I/O control-connections",
        0,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

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
          connect(smartIoControlConnectionSock.get(),
            (const struct sockaddr *)&listenAddr, sizeof(listenAddr)));
      }
        
      // Act as the BridgeProtocolEngine and accept the disk/tape I/O
      // control-connection from the rtcpd daemon
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the I/O control-connection connect event",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the new I/O control-connection has been counted",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Act as the rtcpd daemon and send a request for more work using the
      // newly created disk/tape I/O control-connection
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check writeRTCP_REQUEST_MORE_WORK()",
        unittest::writeRTCP_REQUEST_MORE_WORK(
          smartIoControlConnectionSock.get(),
          volReqId,
          tapePath));

      // Act as the BridgeProtocolEngine and handle the first
      // RTCP_REQUEST_MORE_WORK message from the rtcpd daemon
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first RTCP_REQUEST_MORE_WORK message",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Act as the client and accept the connection for more work from the
      // BridgeProtocolEngine
      const int acceptTimeout = 1;
      int clientConnection1Fd = -1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check accept of first client-connection from the BridgeProtcolEngine",
         clientConnection1Fd = unittest::netAcceptConnection(
           smartClientListenSock.get(), acceptTimeout));
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
          (u_signed64)mountTransactionId,
          moreWorkRequest->mountTransactionId());
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Check maxFiles of request for more migration-work",
          bulkRequestConfigParams.getBulkRequestMigrationMaxFiles().getValue(),
          (uint64_t)moreWorkRequest->maxFiles());
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "Check maxBytes of request for more migration-work",
          bulkRequestConfigParams.getBulkRequestMigrationMaxBytes().getValue(),
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
      filesToMigrateList.setMountTransactionId(mountTransactionId);
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
      const int ioControlConnectionSockFd = smartIoControlConnectionSock.get();
      std::pair<int, int> ioControlConnectionSockFdAndNMsgs(
        ioControlConnectionSockFd, 3);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Create local rtcpd thread to read and drop 3 messages",
        0,
        pthread_create(&localRtcpdThread, localRtcpdThreadAttr,
          unittest::readInAckAndDropNRtcopyMsgs,
          (void *)&ioControlConnectionSockFdAndNMsgs));

      // Act as the BridgeProtocolEngine and handle the first file to be
      // migrated from the client
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file to be migrated from the client",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Join with the rtcpd thread
      void *localRtcpdThreadVoidResult = NULL; 
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Join with local rtcpd thread that read and dropped 3 messages",
        0,
        pthread_join(localRtcpdThread, &localRtcpdThreadVoidResult));

      // Act as the rtcpd daemon and read in from the disk/tape-I/O
      // control-connection socket the delayed ACK of the request for more work
      {
         char ackBuf[12];

         CPPUNIT_ASSERT_EQUAL_MESSAGE(
           "Read in delayed ACK of the request for more work",
           (ssize_t)sizeof(ackBuf),
           read(ioControlConnectionSockFd, ackBuf, sizeof(ackBuf)));
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
            smartIoControlConnectionSock.get(),
            volReqId,
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
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file being migrated",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Act as the rtcpd daemon and read back the ACK from the
      // BridgeProtocolEngine
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check read back of ACK of RTCP_FINISHED message",
        unittest::readAck(smartIoControlConnectionSock.get(), RTCOPY_MAGIC,
          RTCP_FILEERR_REQ, 0));

      // Act as the rtcpd daemon and send the BridgeProtocolEngine a
      // flushed-to-tape message
      {
        tapeBridgeFlushedToTapeMsgBody_t flushedMsgBody;
        memset(&flushedMsgBody, '\0', sizeof(flushedMsgBody));
        flushedMsgBody.volReqId = volReqId;
        flushedMsgBody.tapeFseq = tapeFSeqOfFirstFileToMigrate;

        CPPUNIT_ASSERT_MESSAGE(
          "Check tapebridge_sendTapeBridgeFlushedToTape()",
          0 < tapebridge_sendTapeBridgeFlushedToTape(
            smartIoControlConnectionSock.get(), netTimeout, &flushedMsgBody));
      }

      // Act as the BridgeProtocolEngine and handle the flushed-to-tape message
      // sent by the rtcpd daemon
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first flushed-to-tape message",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

      // Act as the rtcpd daemon and read in the ACK from the
      // BridgeProtocolEngine of the flushed-to-tape message
      {
        tapeBridgeFlushedToTapeAckMsg_t flushedAckMsg;
        memset(&flushedAckMsg, '\0', sizeof(flushedAckMsg));

        CPPUNIT_ASSERT_MESSAGE(
          "Check tapebridge_recvTapeBridgeFlushedToTapeAck()",
          0 <= tapebridge_recvTapeBridgeFlushedToTapeAck(
            smartIoControlConnectionSock.get(), netTimeout, &flushedAckMsg));
      }

      // Act as the client and accept the second connection from the
      // BridgeProtocolEngine.  This connection will be used by the
      // BridgeProtocolEngine to send the FileMigrationReportList message of
      // the first migrated file.
      int clientConnection2Fd = -1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check accept of second client-connection from the BridgeProtcolEngine",
        clientConnection2Fd = unittest::netAcceptConnection(
          smartClientListenSock.get(), acceptTimeout));
      castor::io::AbstractTCPSocket clientMarshallingSock2(clientConnection2Fd);
      clientMarshallingSock2.setTimeout(1);

      // The client is now slow to process the second connection from the
      // BridgeProtocolEngine so does nothing at this very moment in time.

      // Act as the rtcpd daemon and send a request for more work using the
      // the already existant disk/tape I/O control-connection
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check writeRTCP_REQUEST_MORE_WORK()",
        unittest::writeRTCP_REQUEST_MORE_WORK(
          smartIoControlConnectionSock.get(),
          volReqId,
          tapePath));

      // Act as the BridgeProtocolEngine and handle the second
      // RTCP_REQUEST_MORE_WORK message from the rtcpd daemon
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the second RTCP_REQUEST_MORE_WORK message",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is not being shutdown",
        false,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());

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
        errorReport.setErrorCode(ECANCELED);
        errorReport.setErrorMessage("Error message");

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "Send error report in response to first file migrated",
          clientMarshallingSock2.sendObject(errorReport));

        clientMarshallingSock2.close();
      }

      // Act as the BridgeProtocolEngine and handle the error report from the
      // client about the first migrated-file
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check handling of the first file's error report from the client",
        smartEngine->handleSelectEvents(selectTimeout));
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the there is still only one I/O control-connection",
        1,
        smartEngine->getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check no RTCP_ENDOF_REQ messages have been received",
        (uint32_t)0,
        smartEngine->getNbReceivedENDOF_REQs());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the session with the rtcpd daemon is being gracefully shutdown",
        true,
        smartEngine->shuttingDownRtcpdSession());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpd session has not finished",
        false,
        smartEngine->sessionWithRtcpdIsFinished());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check BridgeProtocolEngine should continue processing sockets",
        true,
        smartEngine->continueProcessingSocks());
    }
  }

  void testGenerateMigrationTapeFileId() {
    TraceableDummyFileCloser fileCloser;
    const int initialRtcpdSock = 12;

    {
      TestingBulkRequestConfigParams      bulkRequestConfigParams;
      TestingTapeFlushConfigParams        tapeFlushConfigParams;
      const Cuuid_t                       &cuuid = nullCuuid;
      const int                           rtcpdListenSock  = 11;
      const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        0, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };
      const tapegateway::Volume volume;
      const uint32_t            nbFilesOnDestinationTape = 0;
      AlwaysFalseBoolFunctor    stoppingGracefully;
      Counter<uint64_t>         tapebridgeTransactionCounter(0);
      const bool                logPeerOfCallbackConnectionsFromRtcpd = false;
      const bool                checkRtcpdIsConnectingFromLocalHost = false;
      DummyClientProxy          clientProxy;
      std::auto_ptr<TestingBridgeProtocolEngine> smartEngine;

      bulkRequestConfigParams.setBulkRequestMigrationMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestMigrationMaxFiles(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxBytes(1,
        ConfigParamSource::UNDEFINED);
      bulkRequestConfigParams.setBulkRequestRecallMaxFiles(1,
        ConfigParamSource::UNDEFINED);

      tapeFlushConfigParams.setTapeFlushMode(TAPEBRIDGE_N_FLUSHES_PER_FILE,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxBytesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);
      tapeFlushConfigParams.setMaxFilesBeforeFlush(1,
        ConfigParamSource::UNDEFINED);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check newTestingBridgeProtocolEngine()",
        smartEngine.reset(newTestingBridgeProtocolEngine(
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
          clientProxy)));

      const uint64_t i               = 0xdeadfacedeadface;
      const char     *expectedResult =  "DEADFACEDEADFACE";
      char dst[CA_MAXPATHLEN+1];
      memset(dst, '\0', sizeof(dst));

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Checking engine.generateMigrationTapeFileId()",
        smartEngine->generateMigrationTapeFileId(i, dst));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check result of engine.generateMigrationTapeFileId()",
        std::string(expectedResult),
        std::string(dst));
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that only one socket has been closed",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that initialRtcpdSock was the socket that was closed",
      initialRtcpdSock,
      fileCloser.m_closedFds.front());
  }

  CPPUNIT_TEST_SUITE(BridgeProtocolEngineTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testShutdownOfProtocolUsingLocalDomain);
  CPPUNIT_TEST(testMigrationToDisabledTapeUsingLocalDomain);
  CPPUNIT_TEST(testGenerateMigrationTapeFileId);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeProtocolEngineTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
