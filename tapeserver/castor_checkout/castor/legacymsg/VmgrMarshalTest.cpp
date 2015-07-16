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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/utils/utils.hpp"
#include "vmgr.h"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_VmgrMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_VmgrMarshalTest, marshalVmgrTapeInfoRqstMsgBody) {
  using namespace castor::legacymsg;
  char buf[29];

  // Marshal entire message (header + body)
  {
    VmgrTapeInfoRqstMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    castor::utils::copyString(srcMsgBody.vid, "333333");
    srcMsgBody.side = 4;

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)29, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)VMGR_MAGIC2, dstHeader.magic);
    ASSERT_EQ((uint32_t)VMGR_QRYTAPE, dstHeader.reqType);
    ASSERT_EQ((uint32_t)29, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    VmgrTapeInfoRqstMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 17; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 29, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ(std::string("333333"), dstMsgBody.vid);
    ASSERT_EQ((uint16_t)4, dstMsgBody.side);
  }
}

TEST_F(castor_legacymsg_VmgrMarshalTest, marshalVmgrTapeMountedMsgBody) {
  using namespace castor::legacymsg;
  char buf[33];

  // Marshal entire message (header + body)
  {
    VmgrTapeMountedMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    castor::utils::copyString(srcMsgBody.vid, "333333");
    srcMsgBody.mode = 4;
    srcMsgBody.jid = 5;

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)33, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)VMGR_MAGIC2, dstHeader.magic);
    ASSERT_EQ((uint32_t)VMGR_TPMOUNTED, dstHeader.reqType);
    ASSERT_EQ((uint32_t)33, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    VmgrTapeMountedMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 21; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 33, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ(std::string("333333"), dstMsgBody.vid);
    ASSERT_EQ((uint16_t)4, dstMsgBody.mode);
    ASSERT_EQ((uint32_t)5, dstMsgBody.jid);
  }
}

TEST_F(castor_legacymsg_VmgrMarshalTest, marshalVmgrQryPoolMsgBody) {
  using namespace castor::legacymsg;
  char buf[27];

  // Marshal entire message (header + body)
  {
    VmgrQryPoolMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    castor::utils::copyString(srcMsgBody.poolName, "345678");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)27, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)VMGR_MAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)VMGR_QRYPOOL, dstHeader.reqType);
    ASSERT_EQ((uint32_t)27, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    VmgrQryPoolMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 15; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 27, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ(std::string("345678"), dstMsgBody.poolName);
  }
}

} // namespace unitTests
