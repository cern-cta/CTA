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

#include "common/remoteFS/RemotePath.hpp"
#include "remotens/MockRemoteNS.hpp"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

namespace unitTests {

class cta_MockRemoteNSTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
}; // class cta_MockRemoteNSTest

TEST_F(cta_MockRemoteNSTest, createNonExistingFile) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  const RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");

  {
    std::unique_ptr<RemoteFileStatus> remoteStat;
    ASSERT_NO_THROW(remoteStat = rns.statFile(rp));
    ASSERT_FALSE(remoteStat.get());
  }

  ASSERT_NO_THROW(rns.createEntry(rp, status));

  {
    std::unique_ptr<RemoteFileStatus> remoteStat;
    ASSERT_NO_THROW(remoteStat = rns.statFile(rp));
    ASSERT_TRUE(remoteStat.get());
    ASSERT_TRUE(S_ISREG(remoteStat->mode));
  }
}

TEST_F(cta_MockRemoteNSTest, createExistingFile) {
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

TEST_F(cta_MockRemoteNSTest, createNonExistingDir) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFDIR|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  const RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/dir");

  {
    std::unique_ptr<RemoteFileStatus> remoteStat;
    ASSERT_NO_THROW(remoteStat = rns.statFile(rp));
    ASSERT_FALSE(remoteStat.get());
  }

  ASSERT_NO_THROW(rns.createEntry(rp, status));

  {
    std::unique_ptr<RemoteFileStatus> remoteStat;
    ASSERT_NO_THROW(remoteStat = rns.statFile(rp));
    ASSERT_TRUE(remoteStat.get());
    ASSERT_TRUE(S_ISDIR(remoteStat->mode));
  }
}

TEST_F(cta_MockRemoteNSTest, createExistingDir) {
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

TEST_F(cta_MockRemoteNSTest, statFile) {
  using namespace cta;

  MockRemoteNS rns;
  const UserIdentity owner(1,2);
  const mode_t mode(S_IFREG|S_IRWXU|S_IRWXG|S_IRWXO);
  const uint64_t size = 50;
  RemoteFileStatus status(owner, mode, size);
  const RemotePath rp("eos:/path/to/file");
  ASSERT_NO_THROW(rns.createEntry(rp, status));
  std::unique_ptr<RemoteFileStatus> result;
  ASSERT_NO_THROW(result = rns.statFile(rp));
  ASSERT_TRUE(result.get());
  ASSERT_EQ(mode, result->mode);
  ASSERT_EQ(owner, result->owner);
  ASSERT_EQ(size, result->size);
}

TEST_F(cta_MockRemoteNSTest, rename) {
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
  std::unique_ptr<RemoteFileStatus> result;
  ASSERT_NO_THROW(result = rns.statFile(new_rp));
  ASSERT_TRUE(result.get());
  ASSERT_EQ(mode, result->mode);
  ASSERT_EQ(owner, result->owner);
  ASSERT_EQ(size, result->size);
}

} // namespace unitTests
