/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "common/exception/Errnum.hpp"
#include "mediachanger/SmartFd.hpp"
#include "mediachanger/io.hpp"

namespace unitTests {

class cta_mediachanger_IoTest: public ::testing::Test {
protected:

  const char *const m_listenSockPath;

  cta_mediachanger_IoTest(): m_listenSockPath("/tmp/listenSockForCastorIoTest") {
    // Do nothing
  }

  void setUp() {
    unlink(m_listenSockPath);
  }

  void tearDown() {
    unlink(m_listenSockPath);
  }

  int createLocalListenSocket(const char *const listenSockPath) {

    // Delete the file to be used for the socket if the file already exists
    unlink(listenSockPath);

    // Create the socket
    cta::SmartFd smartListenSock(socket(PF_LOCAL, SOCK_STREAM, 0));
    if(-1 == smartListenSock.get()) {
      char strErrBuf[256];
      if(0 != strerror_r(errno, strErrBuf, sizeof(strErrBuf))) {
        memset(strErrBuf, '\0', sizeof(strErrBuf));
        strncpy(strErrBuf, "Unknown", sizeof(strErrBuf) - 1);
      }

      std::string errorMessage("Call to socket() failed: ");
      errorMessage += strErrBuf;

      cta::exception::Exception ex;
      ex.getMessage() << errorMessage;
      throw ex;
    }

    // Bind the socket
    {
      struct sockaddr_un listenAddr;
      memset(&listenAddr, '\0', sizeof(listenAddr));
      listenAddr.sun_family = PF_LOCAL;
      strncpy(listenAddr.sun_path, listenSockPath,
        sizeof(listenAddr.sun_path) - 1);

      cta::exception::Errnum::throwOnNonZero(
        bind(smartListenSock.get(), (const struct sockaddr *)&listenAddr,
        sizeof(listenAddr)), "Call to bind() failed: ");
    }

    // Make the socket listen
    {
      const int backlog = 128;
      if(0 != listen(smartListenSock.get(), backlog)) {
        char strErrBuf[256];
        if(0 != strerror_r(errno, strErrBuf, sizeof(strErrBuf))) {
          memset(strErrBuf, '\0', sizeof(strErrBuf));
          strncpy(strErrBuf, "Unknown", sizeof(strErrBuf) - 1);
        }

        std::string errorMessage("Call to listen() failed: ");
        errorMessage += strErrBuf;

        cta::exception::Exception ex;
        ex.getMessage() << errorMessage;
        throw ex;
      }
    }

    return smartListenSock.release();
  }

}; // class cta_mediachanger_IoTest

TEST_F(cta_mediachanger_IoTest, connectWithTimeout) {
  cta::SmartFd smartListenSock;
  ASSERT_NO_THROW(smartListenSock.reset(
    createLocalListenSocket(m_listenSockPath)));

  cta::SmartFd smartClientConnectionSock;
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
    ASSERT_NO_THROW(
      smartClientConnectionSock.reset(cta::mediachanger::connectWithTimeout(
        sockDomain,
        sockType,
        sockProtocol,
        (const struct sockaddr *)&address,
        address_len,
        timeout)));
  }

  cta::SmartFd smartServerConnectionSock;
  {
    const time_t acceptTimeout = 10; // Timeout is in seconds
    ASSERT_NO_THROW(smartServerConnectionSock.reset(
      cta::mediachanger::acceptConnection(smartListenSock.get(), acceptTimeout)));
  }

  // Send message from client to server and check it was sent
  char clientToServerMessageBuf[] =
    "Test message from client to server: HELLO";
  const size_t clientToServerMessageBufLen = sizeof(clientToServerMessageBuf);
  {
    const int timeout = 1; // Time out is in seconds
    ASSERT_NO_THROW(cta::mediachanger::writeBytes(smartClientConnectionSock.get(),
      timeout, clientToServerMessageBufLen, clientToServerMessageBuf));
  }
  {
    const int timeout = 1; // Time out is in seconds
    char serverInputBuf[clientToServerMessageBufLen];
    memset(serverInputBuf, '\0', sizeof(serverInputBuf));
    ASSERT_NO_THROW(cta::mediachanger::readBytes(smartServerConnectionSock.get(),
      timeout, clientToServerMessageBufLen, serverInputBuf));

    ASSERT_EQ(std::string(clientToServerMessageBuf),
      std::string(serverInputBuf));
  }

  // Send message from server to client and check it was sent
  char serverToClientMessageBuf[] =
    "Test message from server to client: BONJOUR";
  const size_t serverToClientMessageBufLen = sizeof(serverToClientMessageBuf);
  {
    const int timeout = 1; // Time out is in seconds
    ASSERT_NO_THROW(cta::mediachanger::writeBytes(smartServerConnectionSock.get(),
      timeout, serverToClientMessageBufLen, serverToClientMessageBuf));
  }
  {
    const int timeout = 1; // Time out is in seconds
    char clientInputBuf[serverToClientMessageBufLen];
    memset(clientInputBuf, '\0', sizeof(clientInputBuf));
    ASSERT_NO_THROW(cta::mediachanger::readBytes(smartClientConnectionSock.get(),
      timeout, serverToClientMessageBufLen, clientInputBuf));

    ASSERT_EQ( std::string(serverToClientMessageBuf),
      std::string(clientInputBuf));
  }
}

TEST_F(cta_mediachanger_IoTest, marshalUint8) {
  const uint8_t v = 0x87;
  char buf[1];
  char *ptr = buf;
    
  memset(buf, '\0', sizeof(buf));
    
  ASSERT_NO_THROW(cta::mediachanger::marshalUint8(v, ptr));
  ASSERT_EQ(buf+1, ptr);
  ASSERT_EQ(0x87 & 0xFF, buf[0] & 0xFF);
}

static void check16BitsWereMarshalledBigEndian(const char *const buf) {
  ASSERT_EQ(0x87 & 0xFF, buf[0] & 0xFF);
  ASSERT_EQ(0x65 & 0xFF, buf[1] & 0xFF);
}

TEST_F(cta_mediachanger_IoTest, marshalInt16) {
  const uint16_t v = 0x8765;
  char buf[2];
  char *ptr = buf;

  memset(buf, '\0', sizeof(buf));

  ASSERT_NO_THROW(cta::mediachanger::marshalInt16(v, ptr));
  ASSERT_EQ(buf+2, ptr);
  check16BitsWereMarshalledBigEndian(buf);
}

TEST_F(cta_mediachanger_IoTest, marshalUint16) {
  const uint16_t v = 0x8765;
  char buf[2];
  char *ptr = buf;

  memset(buf, '\0', sizeof(buf));

  ASSERT_NO_THROW(cta::mediachanger::marshalUint16(v, ptr));
  ASSERT_EQ(buf+2, ptr);
  check16BitsWereMarshalledBigEndian(buf);
}

static void check32BitsWereMarshalledBigEndian(const char *const buf) {
  ASSERT_EQ(0x87 & 0xFF, buf[0] & 0xFF);
  ASSERT_EQ(0x65 & 0xFF, buf[1] & 0xFF);
  ASSERT_EQ(0x43 & 0xFF, buf[2] & 0xFF);
  ASSERT_EQ(0x21 & 0xFF, buf[3] & 0xFF);
}

TEST_F(cta_mediachanger_IoTest, marshalInt32) {
  const int32_t v = 0x87654321;
  char buf[4];
  char *ptr = buf;

  memset(buf, '\0', sizeof(buf));

  ASSERT_NO_THROW(cta::mediachanger::marshalInt32(v, ptr));
  ASSERT_EQ(buf+4, ptr);
  check32BitsWereMarshalledBigEndian(buf);
}

TEST_F(cta_mediachanger_IoTest, marshalUint32) {
  const uint32_t v = 0x87654321;
  char buf[4];
  char *ptr = buf;

  memset(buf, '\0', sizeof(buf));

  ASSERT_NO_THROW(cta::mediachanger::marshalUint32(v, ptr));
  ASSERT_EQ(buf+4, ptr);
  check32BitsWereMarshalledBigEndian(buf);
}

static void check64BitsWereMarshalledBigEndian(const char *const buf) {
  ASSERT_EQ(0x88 & 0xFF, buf[0] & 0xFF);
  ASSERT_EQ(0x77 & 0xFF, buf[1] & 0xFF);
  ASSERT_EQ(0x66 & 0xFF, buf[2] & 0xFF);
  ASSERT_EQ(0x55 & 0xFF, buf[3] & 0xFF);
  ASSERT_EQ(0x44 & 0xFF, buf[4] & 0xFF);
  ASSERT_EQ(0x33 & 0xFF, buf[5] & 0xFF);
  ASSERT_EQ(0x22 & 0xFF, buf[6] & 0xFF);
  ASSERT_EQ(0x11 & 0xFF, buf[7] & 0xFF);
}

TEST_F(cta_mediachanger_IoTest, marshalUint64) {
  const uint64_t v = 0x8877665544332211LL;
  char buf[8];
  char *ptr = buf;
    
  memset(buf, '\0', sizeof(buf));
    
  ASSERT_NO_THROW(cta::mediachanger::marshalUint64(v, ptr));
  ASSERT_EQ(buf+8, ptr);
  check64BitsWereMarshalledBigEndian(buf);
}

static void checkStringWasMarshalled(const char *const buf) {
  ASSERT_EQ('V', buf[0]);
  ASSERT_EQ('a', buf[1]);
  ASSERT_EQ('l', buf[2]);
  ASSERT_EQ('u', buf[3]);
  ASSERT_EQ('e', buf[4]);
  ASSERT_EQ('\0', buf[5]);
  ASSERT_EQ('E', buf[6]);
  ASSERT_EQ('E', buf[7]);
}

TEST_F(cta_mediachanger_IoTest, marshalString) {
  const char *const v = "Value";
  char buf[8];

  memset(buf, 'E', sizeof(buf));

  // Test for exception when string is too long for buffer
  char *ptr = buf;
  size_t bufSize = strlen(v);
  ASSERT_THROW(cta::mediachanger::marshalString(v, ptr, bufSize), cta::exception::Exception);

  ptr = buf;
  bufSize = sizeof(buf);
  ASSERT_NO_THROW(cta::mediachanger::marshalString(v, ptr, bufSize));
  ASSERT_EQ(buf+6, ptr);
  ASSERT_EQ(bufSize, sizeof(buf)-6);
  checkStringWasMarshalled(buf);
}

TEST_F(cta_mediachanger_IoTest, unmarshalUint8) {
  char buf[] = {'\x87'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  uint8_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalUint8(ptr, bufLen, v));
  ASSERT_EQ(buf+1, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ(0x87, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalInt16) {
  char buf[] = {'\x87', '\x65'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  int16_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalInt16(ptr, bufLen, v));
  ASSERT_EQ(buf+2, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ((int16_t)0x8765, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalUint16) {
  char buf[] = {'\x87', '\x65'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  uint16_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalUint16(ptr, bufLen, v));
  ASSERT_EQ(buf+2, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ((uint16_t)0x8765, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalUint32) {
  char buf[] = {'\x87', '\x65', '\x43', '\x21'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  uint32_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalUint32(ptr, bufLen, v));
  ASSERT_EQ(buf+4, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ((uint32_t)0x87654321, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalInt32) {
  char buf[] = {'\x87', '\x65', '\x43', '\x21'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  int32_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalInt32(ptr, bufLen, v));
  ASSERT_EQ(buf+4, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ((int32_t)0x87654321, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalUint64) {
  char buf[] = {'\x88', '\x77', '\x66', '\x55', '\x44', '\x33', '\x22', '\x11'};
  size_t bufLen = sizeof(buf);
  const char *ptr = buf;
  uint64_t v = 0;
  ASSERT_NO_THROW(cta::mediachanger::unmarshalUint64(ptr, bufLen, v));
  ASSERT_EQ(buf+8, ptr);
  ASSERT_EQ((size_t)0, bufLen);
  ASSERT_EQ((uint64_t)0x8877665544332211LL, v);
}

TEST_F(cta_mediachanger_IoTest, unmarshalString) {
  char src[] = {'V', 'a', 'l', 'u', 'e', '\0', 'E', 'E'};
  size_t srcLen = sizeof(src);
  const char *srcPtr = src;
  char dst[6];
  const size_t dstLen = sizeof(dst);

  ASSERT_NO_THROW(cta::mediachanger::unmarshalString(srcPtr, srcLen, dst, dstLen));
  ASSERT_EQ(src+6, srcPtr);
  ASSERT_EQ((size_t)2, srcLen);
  ASSERT_EQ(std::string("Value"), std::string(dst));
}

} // namespace unitTests
