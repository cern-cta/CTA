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
#include "h/serrno.h"

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
public:

  ClientProxyTest() {
  }

  void setUp() {
  }

  void tearDown() {
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

  CPPUNIT_TEST_SUITE(ClientProxyTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testConstructorWithEmptyClientTcpIpHost);
  CPPUNIT_TEST(testConstructorWithTooLongClientTcpIpHost);
  CPPUNIT_TEST(testConstructorWithEmptyDgn);
  CPPUNIT_TEST(testConstructorWithTooLongDgn);
  CPPUNIT_TEST(testConstructorWithEmptyDriveUnit);
  CPPUNIT_TEST(testConstructorWithTooLongDriveUnit);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ClientProxyTest);

} // namespace tapebridge
} // namespace tape  
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXYTEST_HPP
