/******************************************************************************
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

#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/CupvMarshal.hpp"
#include "castor/utils/utils.hpp"
#include "h/Cupv.h"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_CupvMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_CupvMarshalTest, marshalCupvCheckMsgBody) {
  using namespace castor::legacymsg;
  char buf[41]; // Expect message (header + body) to occupy exactly 41 bytes

  // Marshal entire message (header + body)
  {
    CupvCheckMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    srcMsgBody.privUid = 3;
    srcMsgBody.privGid = 4;
    castor::utils::copyString(srcMsgBody.srcHost, "FIVE");
    castor::utils::copyString(srcMsgBody.tgtHost, "SIX");
    srcMsgBody.priv = 7;

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)41, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)CUPV_MAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)CUPV_CHECK, dstHeader.reqType);
    ASSERT_EQ((uint32_t)41, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    CupvCheckMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 29; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 41, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ((uint32_t)3, dstMsgBody.privUid);
    ASSERT_EQ((uint32_t)4, dstMsgBody.privGid);
    ASSERT_EQ(std::string("FIVE"), dstMsgBody.srcHost);
    ASSERT_EQ(std::string("SIX"), dstMsgBody.tgtHost);
    ASSERT_EQ((uint32_t)7, dstMsgBody.priv);
  }
}

} // namespace unitTests
