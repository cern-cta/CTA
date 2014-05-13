/******************************************************************************
 *         castor/legacymsg/GenericMarshalTest.cpp
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
 * @author steven.murray@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/GenericMarshal.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_GenericMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_GenericMarshalTest, marshalGenericReplyMsgBody) {
  using namespace castor::legacymsg;
  char buf[30]; // Expect message (header + body) to occupy exactly 80 bytes
  GenericReplyMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {    
    srcMsgBody.status = 1;
    castor::utils::copyString(srcMsgBody.errorMessage, "Error message");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    const uint32_t srcMagic = 1111;
    const uint32_t srcReqType = 2222;
    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMagic, srcReqType, srcMsgBody));    
    ASSERT_EQ((uint32_t)30, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1111, dstHeader.magic);
    ASSERT_EQ((uint32_t)2222, dstHeader.reqType);    
    ASSERT_EQ((uint32_t)18, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    GenericReplyMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 18; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 30, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.status);
    ASSERT_EQ(std::string("Error message"), dstMsgBody.errorMessage);
  }
}

} // namespace unitTests
