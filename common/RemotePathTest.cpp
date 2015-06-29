/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/RemotePath.hpp"

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

TEST_F(cta_RemotePathTest, raw_path_constructor) {
  using namespace cta;

  RemotePath remotePath("xroot://abc.com:1234:the_file");

  ASSERT_EQ(std::string("xroot://abc.com:1234:the_file"), remotePath.getRaw());
  ASSERT_EQ(std::string("xroot"), remotePath.getScheme());
  ASSERT_EQ(std::string("//abc.com:1234:the_file"), remotePath.getHier());
}

} // namespace unitTests
