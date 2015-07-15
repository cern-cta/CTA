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
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_legacymsg_CommonMarshalTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_legacymsg_CommonMarshalTest, marshalMessageHeader) {
  using namespace castor::legacymsg;
  char buf[12];

  // Marshal message header
  {
    MessageHeader srcMsgHeader;

    srcMsgHeader.magic = 1;
    srcMsgHeader.reqType = 2;
    srcMsgHeader.lenOrStatus = 3;

    size_t bufLen = sizeof(buf);
    size_t headerLen = 0;

    ASSERT_NO_THROW(headerLen = marshal(buf, bufLen, srcMsgHeader));

    ASSERT_EQ((size_t)12, headerLen);
  }

  // Unmarshall message header
  {
    MessageHeader dstHeader;
    const char *bufPtr = buf;
    size_t bufLen = 12; // Length of the message header
    ASSERT_NO_THROW(unmarshal(bufPtr, bufLen, dstHeader));
    ASSERT_EQ(buf + 12, bufPtr);
    ASSERT_EQ((size_t)0, bufLen);

    ASSERT_EQ((uint32_t)1, dstHeader.magic);
    ASSERT_EQ((uint32_t)2, dstHeader.reqType);
    ASSERT_EQ((uint32_t)3, dstHeader.lenOrStatus);
  }
}

} // namespace unitTests
