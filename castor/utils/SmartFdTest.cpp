/******************************************************************************
 *                      castor/utils/SmartFd.cpp
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

#include "castor/utils/SmartFd.hpp"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace unitTests {

class castor_utils_SmartFdTest : public ::testing::Test {
protected:
  static int s_fd;
  static bool s_closedCallbackWasCalled;

  virtual void SetUp() {
    s_fd = -1;
    s_closedCallbackWasCalled = false;
  }

  virtual void TearDown() {
  }

  static void closedCallback(int closedfd) {
    s_fd = closedfd;
    s_closedCallbackWasCalled = true;
  }
};

int castor_utils_SmartFdTest::s_fd = -1;
bool castor_utils_SmartFdTest::s_closedCallbackWasCalled = false;

TEST_F(castor_utils_SmartFdTest, testClosedCallback) {
  using namespace castor::utils;

  ASSERT_EQ(-1, s_fd);
  ASSERT_EQ(false, s_closedCallbackWasCalled);

  int fd = socket(PF_LOCAL, SOCK_STREAM, 0);
  ASSERT_NE(-1, fd);

  {
    SmartFd sfd(fd);
    sfd.setClosedCallback(closedCallback);
  }
  ASSERT_EQ(fd, s_fd);
  ASSERT_EQ(true, s_closedCallbackWasCalled);
}

} // namespace unitTests
