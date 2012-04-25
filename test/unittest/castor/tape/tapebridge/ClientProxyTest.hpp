/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/ClientProxyTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXYTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXYTEST_HPP 1

#include "castor/tape/tapebridge/ClientAddressTcpIp.hpp"
#include "castor/tape/tapebridge/ClientProxy.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "h/serrno.h"
#include "test/unittest/unittest.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class ClientProxyTest: public CppUnit::TestFixture {
private:

  /**
   * The path of the local socket that the client would listen on for incoming
   * connections from the ClientProxy.
   */
  const char *const m_clientListenSockPath;

public:

  ClientProxyTest():
    m_clientListenSockPath("/tmp/clientListenSockForClientProxyTest") {
    // Do nothing
  }

  void setUp() {
    unlink(m_clientListenSockPath);
  }

  void tearDown() {
    unlink(m_clientListenSockPath);
  }

  void testConstructor() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          clientHost         = "clientHost";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp   clientAddress(clientHost, clientPort);
    const std::string          dgn                = "dgn";
    const std::string          driveUnit          = "unit";
    std::auto_ptr<ClientProxy> smartClientProxy;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(
        cuuid,
        mountTransactionId,
        netTimeout,
        clientAddress,
        dgn,
        driveUnit)));
  }

  void testConstructorWithEmptyClientTcpIpHost() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          dgn                = "dgn";
    const std::string          clientHost         = "";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp   clientAddress(clientHost, clientPort);
    const std::string          driveUnit          = "unit";
    std::auto_ptr<ClientProxy> smartClientProxy;

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(
        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }

  void testConstructorWithTooLongClientTcpIpHost() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const unsigned short       clientPort         = 1234;
    const std::string          dgn                = "dgn";
    const std::string          driveUnit          = "unit";
    std::auto_ptr<ClientProxy> smartClientProxy;

    // Create a client host-name one character longer than the maximum allowed
    std::ostringstream oss;
    for(int i=0; i<(CA_MAXHOSTNAMELEN+1); i++) {
      oss << "X";
    }
    const std::string clientHost = oss.str();
    const ClientAddressTcpIp clientAddress(clientHost, clientPort);

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }

  void testConstructorWithEmptyDgn() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          clientHost         = "clientHost";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp clientAddress(clientHost, clientPort);
    const std::string          dgn                = "";
    const std::string          driveUnit          = "unit";
    std::auto_ptr<ClientProxy> smartClientProxy;

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }
  
  void testConstructorWithTooLongDgn() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          clientHost         = "clientHost";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp clientAddress(clientHost, clientPort);
    const std::string          driveUnit          = "unit";
    std::auto_ptr<ClientProxy> smartClientProxy;

    // Create a device-group-name that is one character longer than the maximum
    // allowed
    std::ostringstream oss;
    for(int i=0; i<(CA_MAXDGNLEN+1); i++) {
      oss << "X";
    }
    const std::string dgn = oss.str();

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(
        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }

  void testConstructorWithEmptyDriveUnit() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          clientHost         = "clientHost";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp clientAddress(clientHost, clientPort);
    const std::string          dgn                = "dgn";
    const std::string          driveUnit          = "";
    std::auto_ptr<ClientProxy> smartClientProxy;

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }

  void testConstructorWithTooLongDriveUnit() {
    const Cuuid_t              &cuuid             = nullCuuid;
    const uint32_t             mountTransactionId = 0;
    const int                  netTimeout         = 0;
    const std::string          clientHost         = "clientHost";
    const unsigned short       clientPort         = 1234;
    const ClientAddressTcpIp clientAddress(clientHost, clientPort);
    const std::string          dgn                = "dgn";
    std::auto_ptr<ClientProxy> smartClientProxy;

    // Create a tape-drive unit-name that is one character longer than the
    // maximum allowed
    std::ostringstream oss;
    for(int i=0; i<(CA_MAXUNMLEN+1); i++) {
      oss << "X";
    }
    const std::string driveUnit = oss.str();

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(
        cuuid, 
        mountTransactionId,
        netTimeout, 
        clientAddress, 
        dgn, 
        driveUnit)),
      castor::exception::InvalidArgument);
  }

  void testSend1000FilesToMigrateListRequestMessages() {
    // Create the socket on which the client would listen for incoming
    // connections from the ClientProxy
    utils::SmartFd smartClientListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local client listen socket",
       smartClientListenSock.reset(
        unittest::createLocalListenSocket(m_clientListenSockPath)));

    // Create a client proxy
    const Cuuid_t              &cuuid = nullCuuid;
    const uint64_t             mountTransactionId = 1234;
    const int                  netTimeout = 1;
    ClientAddressLocal         clientAddress(m_clientListenSockPath);
    const std::string          dgn("dgn");
    const std::string          driveUnit("unit");
    std::auto_ptr<ClientProxy> smartClientProxy;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ClientProxy()",
      smartClientProxy.reset(new ClientProxy(
        cuuid,
        mountTransactionId,
        netTimeout,
        clientAddress,
        dgn,
        driveUnit)));

    // Send and receive the FilesToMigrateListRequest message 1000 times
    for(int i=0; i<1000; i++) {

      // Use the ClientProxy to send a FilesToMigrateListRequest message
      const uint64_t aggregatorTransactionId = 1;
      const uint64_t maxFiles                = 2;
      const uint64_t maxBytes                = 3;
      smartClientProxy->sendFilesToMigrateListRequest(aggregatorTransactionId,
        maxFiles, maxBytes);

      // Act as the client and accept the connection for more work from the
      // BridgeProtocolEngine
      int clientConnectionFd = -1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Check accept of the connection from the ClientProxy",
         clientConnectionFd = unittest::netAcceptConnection(
           smartClientListenSock.get(), netTimeout));
      castor::io::AbstractTCPSocket clientMarshallingSock(clientConnectionFd);
      clientMarshallingSock.setTimeout(1);

      // Act as the client and read out the FilesToMigrateListRequest message
      std::auto_ptr<IObject> moreWorkRequestObj;
      tapegateway::FilesToMigrateListRequest *moreWorkRequest = NULL;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Read in the FilesToMigrateListRequest message",
        moreWorkRequestObj.reset(clientMarshallingSock.readObject()));
      CPPUNIT_ASSERT_MESSAGE(
        "Check the FilesToMigrateListRequest message was read in",
        NULL != moreWorkRequestObj.get());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check the FilesToMigrateListRequest message is of the correct type",
        (int)castor::OBJ_FilesToMigrateListRequest,
        moreWorkRequestObj->type());
      moreWorkRequest = dynamic_cast<tapegateway::FilesToMigrateListRequest*>
        (moreWorkRequestObj.get());
      CPPUNIT_ASSERT_MESSAGE(
        "Check the dynamic_cast to FilesToMigrateListRequest",
        NULL != moreWorkRequest);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check mountTransactionId of the FilesToMigrateListRequest message",
        mountTransactionId,
        (const uint64_t)moreWorkRequest->mountTransactionId());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxFiles of the FilesToMigrateListRequest message",
        maxFiles,
        (const uint64_t)moreWorkRequest->maxFiles());
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check maxBytes of the FilesToMigrateListRequest message",
        maxBytes,
        (const uint64_t)moreWorkRequest->maxBytes());
    }
  }

  CPPUNIT_TEST_SUITE(ClientProxyTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testConstructorWithEmptyClientTcpIpHost);
  CPPUNIT_TEST(testConstructorWithTooLongClientTcpIpHost);
  CPPUNIT_TEST(testConstructorWithEmptyDgn);
  CPPUNIT_TEST(testConstructorWithTooLongDgn);
  CPPUNIT_TEST(testConstructorWithEmptyDriveUnit);
  CPPUNIT_TEST(testConstructorWithTooLongDriveUnit);
  CPPUNIT_TEST(testSend1000FilesToMigrateListRequestMessages);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ClientProxyTest);

} // namespace tapebridge
} // namespace tape  
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXYTEST_HPP
