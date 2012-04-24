/******************************************************************************
 *                test/unittest/castor/tape/utils/NetTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_NET_NETTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_NET_NETTEST_HPP 1

#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "test/unittest/test_exception.hpp"
#include "test/unittest/unittest.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace castor {
namespace tape   {
namespace net    {

class NetTest: public CppUnit::TestFixture {
private:

  const char *const m_listenSockPath;

  /**
   * Wrapper around castor::tape::net::connectWithTimeOut() that converts
   * anything throw into a std::exception.
   */
  int connectWithTimeoutStdException(
    const int             sockDomain,
    const int             sockType,
    const int             sockProtocol,
    const struct sockaddr *address,
    const socklen_t       address_len,
    const int             timeout)
    throw(std::exception) {
    try {
      return net::connectWithTimeout(
        sockDomain,
        sockType,
        sockProtocol,
        address,
        address_len,
        timeout);
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

  NetTest():
    m_listenSockPath("/tmp/listenSockForNetTest") {
    // Do nothing
  }

  void setUp() {
    unlink(m_listenSockPath);
  }

  void tearDown() {
    unlink(m_listenSockPath);
  }

  void testConnectWithTimeout() {
    utils::SmartFd smartListenSock;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Create local listen socket",
      smartListenSock.reset(
        unittest::createLocalListenSocket(m_listenSockPath)));

    utils::SmartFd smartClientConnectionSock;
    {
      const int             sockDomain   = PF_LOCAL;
      const int             sockType     = SOCK_STREAM;
      const int             sockProtocol = 0;
      struct sockaddr_un    address;
      const socklen_t       address_len  = sizeof(address);
      const int             timeout      = 10; // 10 seconds

      memset(&address, '\0', sizeof(address));
      address.sun_family = PF_LOCAL;
      strncpy(address.sun_path, m_listenSockPath, sizeof(address.sun_path) - 1);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Create connection",
        smartClientConnectionSock.reset(connectWithTimeoutStdException(
          sockDomain,
          sockType,
          sockProtocol,
          (const struct sockaddr *)&address,
          address_len,
          timeout)));
    }

    utils::SmartFd smartServerConnectionSock;
    {
      const time_t acceptTimeout = 10; // Timeout is in seconds
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Accept connection",
        smartServerConnectionSock.reset(net::acceptConnection(
          smartListenSock.get(), acceptTimeout)));
    }

    // Send message from client to server and check it was sent
    char clientToServerMessageBuf[] =
      "Test message from client to server: HELLO";
    const size_t clientToServerMessageBufLen = sizeof(clientToServerMessageBuf);
    {
      const int timeout = 1; // Time out is in seconds
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Writing message from client to server",
        net::writeBytes(smartClientConnectionSock.get(), timeout,
          clientToServerMessageBufLen, clientToServerMessageBuf));
    }
    {
      const int timeout = 1; // Time out is in seconds
      char serverInputBuf[clientToServerMessageBufLen];
      memset(serverInputBuf, '\0', sizeof(serverInputBuf));
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Reading message from client",
        net::readBytes(smartServerConnectionSock.get(), timeout,
          clientToServerMessageBufLen, serverInputBuf));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking message was sent correctly from client to server",
        std::string(clientToServerMessageBuf),
        std::string(serverInputBuf));
    }

    // Send message from server to client and check it was sent
    char serverToClientMessageBuf[] =
      "Test message from server to client: BONJOUR";
    const size_t serverToClientMessageBufLen = sizeof(serverToClientMessageBuf);
    {
      const int timeout = 1; // Time out is in seconds
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Writing message from server to client",
        net::writeBytes(smartServerConnectionSock.get(), timeout,
          serverToClientMessageBufLen, serverToClientMessageBuf));
    }
    {
      const int timeout = 1; // Time out is in seconds
      char clientInputBuf[serverToClientMessageBufLen];
      memset(clientInputBuf, '\0', sizeof(clientInputBuf));
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Reading message from server",
        net::readBytes(smartClientConnectionSock.get(), timeout,
          serverToClientMessageBufLen, clientInputBuf));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking message was sent correctly from server to client",
        std::string(serverToClientMessageBuf),
        std::string(clientInputBuf));
    }
  }

  CPPUNIT_TEST_SUITE(NetTest);
  CPPUNIT_TEST(testConnectWithTimeout);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(NetTest);

} // namespace net
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_NET_NETTEST_HPP
