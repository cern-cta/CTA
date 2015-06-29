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
#include "remotens/MockRemoteNS.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_MockRemoteNsTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class cta_MockRemoteNsTest

TEST_F(cta_MockRemoteNsTest, createNonExistingFile) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  const RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");
  bool regularFileExists = true;
  ASSERT_NO_THROW(regularFileExists = rns.regularFileExists(rp));
  ASSERT_FALSE(regularFileExists);
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  regularFileExists = false;
  ASSERT_NO_THROW(regularFileExists = rns.regularFileExists(rp));
  ASSERT_TRUE(regularFileExists);
}

TEST_F(cta_MockRemoteNsTest, createExistingFile) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  const RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  ASSERT_THROW(rns.createEntry(rp, status), std::exception);
}

TEST_F(cta_MockRemoteNsTest, createNonExistingDir) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFDIR|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  const RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/dir");
  bool dirExists = true;
  ASSERT_NO_THROW(dirExists = rns.dirExists(rp));
  ASSERT_FALSE(dirExists);
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  dirExists = false;
  ASSERT_NO_THROW(dirExists = rns.dirExists(rp));
  ASSERT_TRUE(dirExists);
}

TEST_F(cta_MockRemoteNsTest, createExistingDir) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFDIR|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/dir");
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  ASSERT_THROW(rns.createEntry(rp, status), std::exception);
}

TEST_F(cta_MockRemoteNsTest, statFile) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  RemoteFileStatus result;
  ASSERT_NO_THROW(result = rns.statFile(rp));
  ASSERT_TRUE(result.getMode()==mode);
  ASSERT_TRUE(result.getOwner()==owner);
  ASSERT_TRUE(result.getSize()==size);
}

TEST_F(cta_MockRemoteNsTest, rename) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  const RemotePath new_rp("eos:/path/to/file2");
  ASSERT_NO_THROW(rns.rename(rp, new_rp));  
  RemoteFileStatus result;
  ASSERT_NO_THROW(result = rns.statFile(new_rp));
  ASSERT_TRUE(result.getMode()==mode);
  ASSERT_TRUE(result.getOwner()==owner);
  ASSERT_TRUE(result.getSize()==size);
}

} // namespace unitTests
