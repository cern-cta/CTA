/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
