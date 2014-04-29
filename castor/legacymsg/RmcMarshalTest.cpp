/******************************************************************************
 *         castor/server/RmcMarshalTest.cpp
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
#include "castor/legacymsg/RmcMarshal.hpp"
#include "castor/utils/utils.hpp"
#include "h/rmc_constants.h"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_RmcMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_RmcMarshalTest, marshalRmcAcsMntMsgBody) {
  using namespace castor::legacymsg;
  char buf[40]; // Expect message (header + body) to occupy exactly 40 bytes

  // Marshal entire message (header + body)
  {
    RmcAcsMntMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    srcMsgBody.acs = 3;
    srcMsgBody.lsm = 4;
    srcMsgBody.panel = 5;
    srcMsgBody.transport = 6;
    castor::utils::copyString(srcMsgBody.vid, "VID");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)40, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)RMC_MAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)RMC_ACS_MOUNT, dstHeader.reqType);
    ASSERT_EQ((uint32_t)28, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    RmcAcsMntMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 28; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 40, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ((uint32_t)3, dstMsgBody.acs);
    ASSERT_EQ((uint32_t)4, dstMsgBody.lsm);
    ASSERT_EQ((uint32_t)5, dstMsgBody.panel);
    ASSERT_EQ((uint32_t)6, dstMsgBody.transport);
    ASSERT_EQ(std::string("VID"), dstMsgBody.vid);
  }
}

TEST_F(castor_legacymsg_RmcMarshalTest, marshalRmcMountMsgBody) {
  using namespace castor::legacymsg;
  char buf[29]; // Expect message (header + body) to occupy exactly 29 bytes

  // Marshal entire message (header + body)
  {
    RmcMountMsgBody srcMsgBody;

    srcMsgBody.uid = 1;
    srcMsgBody.gid = 2;
    castor::utils::copyString(srcMsgBody.vid, "VID");
    srcMsgBody.side = 3;
    srcMsgBody.drvOrd = 4;

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

    ASSERT_EQ((uint32_t)RMC_MAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)RMC_MOUNT, dstHeader.reqType);
    ASSERT_EQ((uint32_t)29, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    RmcMountMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 17; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 29, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)2, dstMsgBody.gid);
    ASSERT_EQ(std::string(""), dstMsgBody.unusedLoader);
    ASSERT_EQ(std::string("VID"), dstMsgBody.vid);
    ASSERT_EQ((uint16_t)3, dstMsgBody.side);
    ASSERT_EQ((uint16_t)4, dstMsgBody.drvOrd);
  }
}

} // namespace unitTests
