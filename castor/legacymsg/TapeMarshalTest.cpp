/******************************************************************************
 *         castor/legacymsg/TapeMarshalTest.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/utils/utils.hpp"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"
#include "h/serrno.h"

#include <gtest/gtest.h>

namespace unitTests {

class castor_server_TapeMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_server_TapeMarshalTest, marshalTapeConfigRequestMsgBody) {
  using namespace castor::legacymsg;
  char buf[80]; // Expect message (header + body) to occupy exactly 80 bytes
  TapeConfigRequestMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {    
    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    castor::utils::copyString(srcMsgBody.drive, "HELLO");
    srcMsgBody.status = 4;

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));    
    ASSERT_EQ((uint32_t)28, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)TPMAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)TPCONF, dstHeader.reqType);    
    ASSERT_EQ((uint32_t)28, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    TapeConfigRequestMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 16; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 28, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ(std::string("HELLO"), dstMsgBody.drive);
    ASSERT_EQ((int16_t)4, dstMsgBody.status);
  }
}

TEST_F(castor_server_TapeMarshalTest, marshalTapeStatRequestMsgBody) {
  using namespace castor::legacymsg;
  char buf[80]; // Expect message (header + body) to occupy exactly 80 bytes
  TapeStatRequestMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {
    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));
    ASSERT_EQ((uint32_t)20, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)TPMAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)TPSTAT, dstHeader.reqType);
    ASSERT_EQ((uint32_t)20, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    TapeStatRequestMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 8; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 20, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
  }
}

} // namespace unitTests
