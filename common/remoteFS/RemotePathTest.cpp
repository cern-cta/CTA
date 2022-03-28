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

#include "common/remoteFS/RemotePath.hpp"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace unitTests {

class cta_RemotePathTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_RemotePathTest, default_constructor) {
  using namespace cta;

  RemotePath remotePath;
  ASSERT_TRUE(remotePath.empty());
  ASSERT_THROW(remotePath.getRaw(), std::exception);
  ASSERT_THROW(remotePath.getScheme(), std::exception);
  ASSERT_THROW(remotePath.getAfterScheme(), std::exception);
}

TEST_F(cta_RemotePathTest, raw_path_constructor) {
  using namespace cta;

  const RemotePath remotePath("xroot://abc.com:1234:the_file");

  ASSERT_FALSE(remotePath.empty());
  ASSERT_EQ(std::string("xroot://abc.com:1234:the_file"), remotePath.getRaw());
  ASSERT_EQ(std::string("xroot"), remotePath.getScheme());
  ASSERT_EQ(std::string("//abc.com:1234:the_file"), remotePath.getAfterScheme());
}

TEST_F(cta_RemotePathTest, raw_path_constructor_empty_string) {
  using namespace cta;

  RemotePath remotePath;

  ASSERT_THROW(remotePath = RemotePath(""), std::exception);
}

TEST_F(cta_RemotePathTest, raw_path_constructor_just_a_colon) {
  using namespace cta;

  RemotePath remotePath;

  ASSERT_THROW(remotePath = RemotePath(":"), std::exception);
}

TEST_F(cta_RemotePathTest, raw_path_constructor_no_scheme) {
  using namespace cta;

  RemotePath remotePath;

  ASSERT_THROW(remotePath = RemotePath("://abc.com:1234:the_file"), std::exception);
}

TEST_F(cta_RemotePathTest, raw_path_constructor_nothing_after_scheme) {
  using namespace cta;

  RemotePath remotePath;

  ASSERT_THROW(remotePath = RemotePath("xroot:"), std::exception);
}

} // namespace unitTests
