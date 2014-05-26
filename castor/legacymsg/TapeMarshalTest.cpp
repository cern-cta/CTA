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

class castor_legacymsg_TapeMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_TapeMarshalTest, marshalTapeConfigRequestMsgBody) {
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

TEST_F(castor_legacymsg_TapeMarshalTest, marshalTapeStatRequestMsgBody) {
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

TEST_F(castor_legacymsg_TapeMarshalTest, marshalTapeUpdateDriveRqstMsgBody) {
  using namespace castor::legacymsg;
  char buf[37]; // Expect message (header + body) to occupy exactly 25 bytes
  TapeUpdateDriveRqstMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {
    srcMsgBody.event = 1;
    srcMsgBody.mode = 2;
    srcMsgBody.clientType = 3;
    
    castor::utils::copyString(srcMsgBody.vid, "VIDVID");
    castor::utils::copyString(srcMsgBody.drive, "DRIVE");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));
    ASSERT_EQ((uint32_t)37, totalLen);
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
    ASSERT_EQ((uint32_t)UPDDRIVE, dstHeader.reqType);
    ASSERT_EQ((uint32_t)37, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    TapeUpdateDriveRqstMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 25; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 37, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstMsgBody.event);
    ASSERT_EQ((uint32_t)2, dstMsgBody.mode);
    ASSERT_EQ((uint32_t)3, dstMsgBody.clientType);
    
    ASSERT_EQ(std::string("VIDVID"), dstMsgBody.vid);
    ASSERT_EQ(std::string("DRIVE"), dstMsgBody.drive);
  }
}

TEST_F(castor_legacymsg_TapeMarshalTest, marshalTapeLabelRqstMsgBody) {
  using namespace castor::legacymsg;
  char buf[45]; // Expect message (header + body) to occupy exactly 27 bytes
  TapeLabelRqstMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {
    srcMsgBody.force = 0;
    srcMsgBody.uid = 3;
    srcMsgBody.gid = 4;
    castor::utils::copyString(srcMsgBody.vid, "VIDVID");
    castor::utils::copyString(srcMsgBody.drive, "DRIVE001");
    castor::utils::copyString(srcMsgBody.dgn, "DGNDGN");

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));
    ASSERT_EQ((uint32_t)45, totalLen);
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
    ASSERT_EQ((uint32_t)TPLABEL, dstHeader.reqType);
    ASSERT_EQ((uint32_t)45, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    TapeLabelRqstMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 33; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 45, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint16_t)0, dstMsgBody.force);
    ASSERT_EQ((uint32_t)3, dstMsgBody.uid);
    ASSERT_EQ((uint32_t)4, dstMsgBody.gid);
    ASSERT_EQ(std::string("VIDVID"), dstMsgBody.vid);
    ASSERT_EQ(std::string("DRIVE001"), dstMsgBody.drive);
    ASSERT_EQ(std::string("DGNDGN"), dstMsgBody.dgn);
  }
}

TEST_F(castor_legacymsg_TapeMarshalTest, marshalTapeStatReplyMsgBody) {
  using namespace castor::legacymsg;
  char buf[130]; // Expect message (header + body) to occupy exactly 27 bytes
  TapeStatReplyMsgBody srcMsgBody;

  // Marshal entire message (header + body)
  {
    srcMsgBody.number_of_drives = 2; //2 bytes
    
    srcMsgBody.drives[0].asn = 1;
    srcMsgBody.drives[0].asn_time = 2;
    srcMsgBody.drives[0].cfseq = 3;
    castor::utils::copyString(srcMsgBody.drives[0].dgn, "DGN000");
    castor::utils::copyString(srcMsgBody.drives[0].drive, "DRIVE000");
    srcMsgBody.drives[0].jid = 4;
    castor::utils::copyString(srcMsgBody.drives[0].lblcode, "L00");
    srcMsgBody.drives[0].mode = 5;
    srcMsgBody.drives[0].tobemounted = 6;
    srcMsgBody.drives[0].uid = 7;
    srcMsgBody.drives[0].up = 8;
    castor::utils::copyString(srcMsgBody.drives[0].vid, "VID000");
    castor::utils::copyString(srcMsgBody.drives[0].vsn, "VSN000"); //58 bytes
    
    srcMsgBody.drives[1].asn = 11;
    srcMsgBody.drives[1].asn_time = 12;
    srcMsgBody.drives[1].cfseq = 13;
    castor::utils::copyString(srcMsgBody.drives[1].dgn, "DGN001");
    castor::utils::copyString(srcMsgBody.drives[1].drive, "DRIVE001");
    srcMsgBody.drives[1].jid = 14;
    castor::utils::copyString(srcMsgBody.drives[1].lblcode, "L01");
    srcMsgBody.drives[1].mode = 15;
    srcMsgBody.drives[1].tobemounted = 16;
    srcMsgBody.drives[1].uid = 17;
    srcMsgBody.drives[1].up = 18;
    castor::utils::copyString(srcMsgBody.drives[1].vid, "VID001");
    castor::utils::copyString(srcMsgBody.drives[1].vsn, "VSN001"); //58 bytes

    size_t bufLen = sizeof(buf);
    size_t totalLen = 0; // Total length of message (header + body)

    ASSERT_NO_THROW(totalLen = marshal(buf, bufLen, srcMsgBody));
    ASSERT_EQ((uint32_t)130, totalLen);
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
    ASSERT_EQ((uint32_t)MSG_DATA, dstHeader.reqType);
    ASSERT_EQ((uint32_t)118, dstHeader.lenOrStatus);
  }

  // Unmarshall message body
  {
    TapeStatReplyMsgBody dstMsgBody;

    const char *bufPtr = buf + 12; // Point at beginning of message body
    size_t bufLen = 118; // Length of the message body
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstMsgBody));
    ASSERT_EQ(buf + 130, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);
    
    ASSERT_EQ((uint16_t)2, dstMsgBody.number_of_drives); //2 bytes
    
    ASSERT_EQ((uint16_t)1, dstMsgBody.drives[0].asn);
    ASSERT_EQ((uint32_t)2, dstMsgBody.drives[0].asn_time);
    ASSERT_EQ((uint32_t)3, dstMsgBody.drives[0].cfseq);
    ASSERT_EQ(std::string("DGN000"), dstMsgBody.drives[0].dgn);
    ASSERT_EQ(std::string("DRIVE000"), dstMsgBody.drives[0].drive);
    ASSERT_EQ((uint32_t)4, dstMsgBody.drives[0].jid);
    ASSERT_EQ(std::string("L00"), dstMsgBody.drives[0].lblcode);
    ASSERT_EQ((uint16_t)5, dstMsgBody.drives[0].mode);
    ASSERT_EQ((uint16_t)6, dstMsgBody.drives[0].tobemounted);
    ASSERT_EQ((uint32_t)7, dstMsgBody.drives[0].uid);
    ASSERT_EQ((uint16_t)8, dstMsgBody.drives[0].up);
    ASSERT_EQ(std::string("VID000"), dstMsgBody.drives[0].vid);
    ASSERT_EQ(std::string("VSN000"), dstMsgBody.drives[0].vsn); //51 bytes
    
    ASSERT_EQ((uint16_t)11, dstMsgBody.drives[1].asn);
    ASSERT_EQ((uint32_t)12, dstMsgBody.drives[1].asn_time);
    ASSERT_EQ((uint32_t)13, dstMsgBody.drives[1].cfseq);
    ASSERT_EQ(std::string("DGN001"), dstMsgBody.drives[1].dgn);
    ASSERT_EQ(std::string("DRIVE001"), dstMsgBody.drives[1].drive);
    ASSERT_EQ((uint32_t)14, dstMsgBody.drives[1].jid);
    ASSERT_EQ(std::string("L01"), dstMsgBody.drives[1].lblcode);
    ASSERT_EQ((uint16_t)15, dstMsgBody.drives[1].mode);
    ASSERT_EQ((uint16_t)16, dstMsgBody.drives[1].tobemounted);
    ASSERT_EQ((uint32_t)17, dstMsgBody.drives[1].uid);
    ASSERT_EQ((uint16_t)18, dstMsgBody.drives[1].up);
    ASSERT_EQ(std::string("VID001"), dstMsgBody.drives[1].vid);
    ASSERT_EQ(std::string("VSN001"), dstMsgBody.drives[1].vsn);
  }
}

} // namespace unitTests
