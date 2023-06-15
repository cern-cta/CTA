/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/SmartFd.hpp"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace unitTests {

class cta_SmartFdTest : public ::testing::Test {
protected:
  static int s_fd;
  static bool s_closedCallbackWasCalled;

  virtual void SetUp() {
    s_fd = -1;
    s_closedCallbackWasCalled = false;
  }

  virtual void TearDown() {}

  static void closedCallback(int closedfd) {
    s_fd = closedfd;
    s_closedCallbackWasCalled = true;
  }
};

int cta_SmartFdTest::s_fd = -1;
bool cta_SmartFdTest::s_closedCallbackWasCalled = false;

TEST_F(cta_SmartFdTest, testClosedCallback) {
  using namespace cta;

  ASSERT_EQ(-1, s_fd);
  ASSERT_FALSE(s_closedCallbackWasCalled);

  int fd = socket(PF_LOCAL, SOCK_STREAM, 0);
  ASSERT_NE(-1, fd);

  {
    SmartFd sfd(fd);
    sfd.setClosedCallback(closedCallback);
  }
  ASSERT_EQ(fd, s_fd);
  ASSERT_EQ(true, s_closedCallbackWasCalled);
}

}  // namespace unitTests
