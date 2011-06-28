/******************************************************************************
 *                test/tapebridge/MarshallTapeBridgeFlushedToTapeTest.hpp
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

#ifndef TEST_TAPEBRIDGE_MARSHALLTAPEBRIDGEFLUSHEDTOTAPE_HPP
#define TEST_TAPEBRIDGE_MARSHALLTAPEBRIDGEFLUSHEDTOTAPE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/TapeBridgeMarshal.hpp"
#include "h/marshall.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>

class MarshallTapeBridgeFlushedToTapeTest: public CppUnit::TestFixture {
private:
  char m_buf[TAPEBRIDGE_MSGBUFSIZ];
  tapeBridgeFlushedToTapeMsgBody_t m_msgBody;
  const int32_t m_sizeOfMarshalledMsgHeader;
  const int32_t m_sizeOfMarshalledMsgBody;
  const int32_t m_sizeOfMarshalledMsgHeaderPlusBody;

public:

  MarshallTapeBridgeFlushedToTapeTest():
    m_sizeOfMarshalledMsgHeader(3 * LONGSIZE), /* magic, reqType, bodyLen */
    m_sizeOfMarshalledMsgBody(2 * LONGSIZE), /* volReqId, tapeFseq */
    m_sizeOfMarshalledMsgHeaderPlusBody(m_sizeOfMarshalledMsgHeader +
      m_sizeOfMarshalledMsgBody) {

    memset(m_buf     , '\0', sizeof(m_buf)    );
    memset(&m_msgBody, '\0', sizeof(m_msgBody));
  }

  void setUp() {
    memset(m_buf     , '\0', sizeof(m_buf)    );
    memset(&m_msgBody, '\0', sizeof(m_msgBody));

    m_msgBody.volReqId = 1111;
    m_msgBody.tapeFseq = 2222;

    serrno = 0;
  }

  void tearDown() {
  }

  void testMarshallBufNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("buf = NULL", -1,
      tapebridge_marshallTapeBridgeFlushedToTapeMsg(NULL, sizeof(m_buf),
      &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for buf = NULL", EINVAL, serrno);
  }

  void testMarshallBufLen0() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 0", -1,
      tapebridge_marshallTapeBridgeFlushedToTapeMsg(m_buf, 0, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 0", EINVAL, serrno);
  }

  void testMarshallBufLen1() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("bufLen = 1", -1,
      tapebridge_marshallTapeBridgeFlushedToTapeMsg(m_buf, 1, &m_msgBody));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for bufLen = 1", EINVAL, serrno);
  }

  void testMarshallMsgBodyNull() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("msgBody = NULL", -1,
      tapebridge_marshallTapeBridgeFlushedToTapeMsg(m_buf, sizeof(m_buf),
      NULL));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("serrno for msgBody = NULL", EINVAL, serrno);
  }

  void testMarshallCorrectCall() {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Correct call",
      m_sizeOfMarshalledMsgHeaderPlusBody,
      tapebridge_marshallTapeBridgeFlushedToTapeMsg(m_buf, sizeof(m_buf),
      &m_msgBody));

    char     *p                   = m_buf;
    uint32_t unmarshalledMagic    = 0;
    uint32_t unmarshalledReqType  = 0;
    uint32_t unmarshalledLen      = 0;
    uint32_t unmarshalledVolReqId = 0;
    uint32_t unmarshalledTapeFseq = 0;

    unmarshall_LONG(p, unmarshalledMagic);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("magic marshalled",
      (uint32_t)RTCOPY_MAGIC,
      unmarshalledMagic);

    unmarshall_LONG(p, unmarshalledReqType);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("reqType marshalled",
      (uint32_t)TAPEBRIDGE_FLUSHEDTOTAPE,
      unmarshalledReqType);

    unmarshall_LONG(p, unmarshalledLen);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("len marshalled",
      (uint32_t)(2 * LONGSIZE),
      unmarshalledLen);

    unmarshall_LONG(p, unmarshalledVolReqId);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("volReqId marshalled",
      m_msgBody.volReqId,
      unmarshalledVolReqId);

    unmarshall_LONG(p, unmarshalledTapeFseq);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("tapeFseq marshalled",
      m_msgBody.tapeFseq,
      unmarshalledTapeFseq);
  }

  void testUnmarshalBufNull() {
    const char *buf = NULL;
    tapeBridgeFlushedToTapeMsgBody_t unmarshalledMsgBody;
    size_t bytesLeftInBuffer = 0;

    memset(&unmarshalledMsgBody, '\0', sizeof(unmarshalledMsgBody));

    CPPUNIT_ASSERT_THROW_MESSAGE("unmarshall NULL buffer",
      castor::tape::legacymsg::unmarshal(buf, bytesLeftInBuffer,
        unmarshalledMsgBody),
      castor::exception::Exception);
  }

  void testUnmarshalCorrectCall() {
    char buf[2 * LONGSIZE]; // volReqId + tapeFseq
    size_t bytesLeftInBuffer = sizeof(buf);

    {
      char *p = buf;
      marshall_LONG(p, m_msgBody.volReqId);
      marshall_LONG(p, m_msgBody.tapeFseq);

      CPPUNIT_ASSERT_EQUAL_MESSAGE("marshalled correct number of bytes",
        size_t(2 * LONGSIZE),
        size_t(p - buf));
    }

    tapeBridgeFlushedToTapeMsgBody_t unmarshalledMsgBody;

    memset(&unmarshalledMsgBody, '\0', sizeof(unmarshalledMsgBody));
    {
      const char *p = buf;
      castor::tape::legacymsg::unmarshal(p, bytesLeftInBuffer,
        unmarshalledMsgBody);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Correct number of bytes left in buffer",
      (size_t)0,
      bytesLeftInBuffer);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("volReqId unmarshalled",
      m_msgBody.volReqId,
      unmarshalledMsgBody.volReqId);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("tapeFseq unmarshalled",
      m_msgBody.tapeFseq,
      unmarshalledMsgBody.tapeFseq);
  }

  CPPUNIT_TEST_SUITE(MarshallTapeBridgeFlushedToTapeTest);
  CPPUNIT_TEST(testMarshallBufNull);
  CPPUNIT_TEST(testMarshallBufLen0);
  CPPUNIT_TEST(testMarshallBufLen1);
  CPPUNIT_TEST(testMarshallMsgBodyNull);
  CPPUNIT_TEST(testMarshallCorrectCall);
  CPPUNIT_TEST(testUnmarshalBufNull);
  CPPUNIT_TEST(testUnmarshalCorrectCall);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(MarshallTapeBridgeFlushedToTapeTest);

#endif // TEST_TAPEBRIDGE_MARSHALLTAPEBRIDGEFLUSHEDTOTAPE_HPP
