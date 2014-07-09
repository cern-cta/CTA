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
#include "castor/legacymsg/VdqmMarshal.hpp"
#include "castor/utils/utils.hpp"
#include "h/vdqm_constants.h"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_VdqmMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_VdqmMarshalTest, marshalVdqmDrvRqstMsgBody) {
  using namespace castor::legacymsg;
  char buf[80]; // Expect message (header + body) to occupy exactly 80 bytes

  // Marshal entire message (header + body)
  {
    VdqmDrvRqstMsgBody srcMsgBody;
    srcMsgBody.status = 1;
    srcMsgBody.drvReqId = 2;
    srcMsgBody.volReqId = 3;
    srcMsgBody.jobId = 4;
    srcMsgBody.recvTime = 5;
    srcMsgBody.resetTime = 6;
    srcMsgBody.useCount = 7;
    srcMsgBody.errCount = 8;
    srcMsgBody.transfMB = 9;
    srcMsgBody.mode = 10;
    srcMsgBody.totalMB = 11;
    castor::utils::copyString(srcMsgBody.volId, "VOL");
    castor::utils::copyString(srcMsgBody.server, "SRV");
    castor::utils::copyString(srcMsgBody.drive, "DRV");
    castor::utils::copyString(srcMsgBody.dgn, "DGN");
    castor::utils::copyString(srcMsgBody.dedicate, "DCT");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));

    ASSERT_EQ((size_t)80, totalLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)VDQM_MAGIC, dstHeader.magic);
    ASSERT_EQ((uint32_t)VDQM_DRV_REQ, dstHeader.reqType);
    ASSERT_EQ((uint32_t)68, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    VdqmDrvRqstMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 68; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 80, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((int32_t)1, dstMsgBody.status);
    ASSERT_EQ((int32_t)2, dstMsgBody.drvReqId);
    ASSERT_EQ((int32_t)3, dstMsgBody.volReqId);
    ASSERT_EQ((int32_t)4, dstMsgBody.jobId);
    ASSERT_EQ((int32_t)5, dstMsgBody.recvTime);
    ASSERT_EQ((int32_t)6, dstMsgBody.resetTime);
    ASSERT_EQ((int32_t)7, dstMsgBody.useCount);
    ASSERT_EQ((int32_t)8, dstMsgBody.errCount);
    ASSERT_EQ((int32_t)9, dstMsgBody.transfMB);
    ASSERT_EQ((int32_t)10, dstMsgBody.mode);
    ASSERT_EQ((uint64_t)11, dstMsgBody.totalMB);
    ASSERT_EQ(std::string("VOL"), dstMsgBody.volId);
    ASSERT_EQ(std::string("SRV"), dstMsgBody.server);
    ASSERT_EQ(std::string("DRV"), dstMsgBody.drive);
    ASSERT_EQ(std::string("DGN"), dstMsgBody.dgn);
    ASSERT_EQ(std::string("DCT"), dstMsgBody.dedicate);
  }
}

} // namespace unitTests
