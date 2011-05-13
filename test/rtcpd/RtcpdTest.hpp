/******************************************************************************
 *                test/rtcpd/RtcpdTest.hpp
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

#ifndef TEST_RTCPD_RTCPDTEST_HPP
#define TEST_RTCPD_RTCPDTEST_HPP 1

#include "h/osdep.h"
#include "h/serrno.h"
#include "h/rtcp_constants.h"
#include "h/rtcp_marshallVdqmClientInfoMsg.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>

class RtcpdTest: public CppUnit::TestFixture {
private:
  char m_buf[RTCP_MSGBUFSIZ];
  vdqmClientInfoMsgBody_t m_msgBody;
  const int32_t m_sizeOfMarshalledMsgHeader;
  int32_t m_sizeOfMarshalledMsgHeaderPlusBody;
  int32_t m_sizeOfMarshalledMsgBody;

public:

  RtcpdTest():
    m_sizeOfMarshalledMsgHeader(3 * LONGSIZE), /* magic, reqType, bodyLen */
    m_sizeOfMarshalledMsgHeaderPlusBody(0),
    m_sizeOfMarshalledMsgBody(0) {

    memset(m_buf     , '\0', sizeof(m_buf)    );
    memset(&m_msgBody, '\0', sizeof(m_msgBody));
  }

  void setUp() {
    memset(m_buf     , 0, sizeof(m_buf)    );
    memset(&m_msgBody, 0, sizeof(m_msgBody));

    m_msgBody.volReqId           = 1111;
    m_msgBody.clientCallbackPort = 2222;
    m_msgBody.clientUID          = 3333;
    m_msgBody.clientGID          = 4444;

    strncpy(m_msgBody.clientHost, "clientHost", sizeof(m_msgBody.clientHost));
    m_msgBody.clientHost[sizeof(m_msgBody.clientHost) - 1] = '\0';

    strncpy(m_msgBody.dgn, "dgn", sizeof(m_msgBody.dgn));
    m_msgBody.dgn[sizeof(m_msgBody.dgn) - 1] = '\0';

    strncpy(m_msgBody.drive, "drive", sizeof(m_msgBody.drive));
    m_msgBody.drive[sizeof(m_msgBody.drive) - 1] = '\0';

    strncpy(m_msgBody.clientName, "clientName", sizeof(m_msgBody.clientName));
    m_msgBody.clientName[sizeof(m_msgBody.clientName) - 1] = '\0';

    m_sizeOfMarshalledMsgBody =
      LONGSIZE + /* volReqID           */
      LONGSIZE + /* clientCallbackPort */
      LONGSIZE + /* clientUID          */
      LONGSIZE + /* clientGID          */
      strlen(m_msgBody.clientHost) + 1 +
      strlen(m_msgBody.dgn)        + 1 +
      strlen(m_msgBody.drive)      + 1 +
      strlen(m_msgBody.clientName) + 1;

    m_sizeOfMarshalledMsgHeaderPlusBody =
      m_sizeOfMarshalledMsgHeader +
      m_sizeOfMarshalledMsgBody;

    serrno = 0;
  }

  void tearDown() {
  }

  void testMsgBodyMarshalledSize() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Message body marshalled size",
      m_sizeOfMarshalledMsgBody,
      rtcp_vdqmClientInfoMsgBodyMarshalledSize(&m_msgBody));
  }

  void testMarshallBufNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("buf = NULL", -1,
      rtcp_marshallVdqmClientInfoMsg(NULL, sizeof(m_buf),
      &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for buf = NULL", EINVAL, serrno);
  }

  void testMarshallBufLen0() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 0", -1,
      rtcp_marshallVdqmClientInfoMsg(m_buf, 0, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 0", EINVAL, serrno);
  }

  void testMarshallBufLen1() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 1", -1,
      rtcp_marshallVdqmClientInfoMsg(m_buf, 1, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 1", EINVAL, serrno);
  }

  void testMarshallMsgBodyNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("msgBody = NULL", -1,
      rtcp_marshallVdqmClientInfoMsg(m_buf, sizeof(m_buf),
      NULL));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for msgBody = NULL", EINVAL, serrno);
  }

  void testMarshallCorrectCall() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Correct call",
      m_sizeOfMarshalledMsgHeaderPlusBody,
      rtcp_marshallVdqmClientInfoMsg(m_buf, sizeof(m_buf),
      &m_msgBody));
  }

  void testUnmarshallBufNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("buf = NULL", -1,
      rtcp_unmarshallVdqmClientInfoMsgBody(NULL, sizeof(m_buf),
      &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for buf = NULL", EINVAL, serrno);
  }

  void testUnmarshallBufLen0() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 0", -1,
      rtcp_unmarshallVdqmClientInfoMsgBody(m_buf, 0, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 0", EINVAL, serrno);
  }

  void testUnmarshallBufLen1() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 1", -1,
      rtcp_unmarshallVdqmClientInfoMsgBody(m_buf, 1, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 1", EINVAL, serrno);
  }

  void testUnmarshallMsgBodyNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("msgBody = NULL", -1,
      rtcp_unmarshallVdqmClientInfoMsgBody(m_buf, sizeof(m_buf),
      NULL));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for msgBody = NULL", EINVAL, serrno);
  }

  void testMarshallAndUnmarshallContents() {
    vdqmClientInfoMsgBody_t msgBody;

    memset(&msgBody, '\0', sizeof(msgBody));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("marshall",
      m_sizeOfMarshalledMsgHeaderPlusBody,
      rtcp_marshallVdqmClientInfoMsg(m_buf, sizeof(m_buf),
      &m_msgBody));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("unmarshall", m_sizeOfMarshalledMsgBody,
      rtcp_unmarshallVdqmClientInfoMsgBody(m_buf +
      m_sizeOfMarshalledMsgHeader, sizeof(m_buf), &msgBody));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("volReqId", m_msgBody.volReqId,
      msgBody.volReqId);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientCallbackPort",
      m_msgBody.clientCallbackPort, msgBody.clientCallbackPort);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientUID", m_msgBody.clientUID,
      msgBody.clientUID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientGID", m_msgBody.clientGID,
      msgBody.clientGID);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientHost",
      std::string(m_msgBody.clientHost),
      std::string(msgBody.clientHost));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("dgn",
      std::string(m_msgBody.dgn),
      std::string(msgBody.dgn));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("drive",
      std::string(m_msgBody.drive),
      std::string(msgBody.drive));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("clientName",
      std::string(m_msgBody.clientName),
      std::string(msgBody.clientName));
  }

  CPPUNIT_TEST_SUITE(RtcpdTest);
  CPPUNIT_TEST(testMsgBodyMarshalledSize);
  CPPUNIT_TEST(testMarshallBufNull);
  CPPUNIT_TEST(testMarshallBufLen0);
  CPPUNIT_TEST(testMarshallBufLen1);
  CPPUNIT_TEST(testMarshallMsgBodyNull);
  CPPUNIT_TEST(testMarshallCorrectCall);
  CPPUNIT_TEST(testUnmarshallBufNull);
  CPPUNIT_TEST(testUnmarshallBufLen0);
  CPPUNIT_TEST(testUnmarshallBufLen1);
  CPPUNIT_TEST(testUnmarshallMsgBodyNull);
  CPPUNIT_TEST(testMarshallAndUnmarshallContents);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(RtcpdTest);

#endif // TEST_RTCPD_RTCPDTEST_HPP
