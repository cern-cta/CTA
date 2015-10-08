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

#include <gtest/gtest.h>
#include "ArchiveToFileRequest.hpp"
#include "BackendVFS.hpp"
#include "Agent.hpp"
#include "CreationLog.hpp"
#include "UserIdentity.hpp"

namespace unitTests {
  
TEST(ObjectStore, ArchiveToFileRequestBasicAccess) {
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTest");
  time_t now;
  std::string tapeAddress = agent.nextId("ArchiveToFileRequest");
  {
    // Try to create the ArchiveToFileRequest entry
    cta::objectstore::ArchiveToFileRequest atfr(tapeAddress, be);
    atfr.initialize();cta::ArchiveFile af;
    af.path = "cta://dir/file";
    atfr.setArchiveFile(af);
    atfr.setRemoteFile(cta::RemotePathAndStatus(cta::RemotePath("eos://dir2/file2"), 
      cta::RemoteFileStatus(cta::UserIdentity(123,456), 0744, 12345678)));
    atfr.setCreationLog(cta::CreationLog(cta::UserIdentity(123,456),"testHost", 
      now=time(NULL), "Comment"));
    atfr.setPriority(12);
    atfr.insert();
  }
  {
    // Try to load back the Request and dump it.
    cta::objectstore::ArchiveToFileRequest atfr(tapeAddress, be);
    cta::objectstore::ScopedSharedLock atfrl(atfr);
    atfr.fetch();
    ASSERT_EQ("cta://dir/file", atfr.getArchiveFile().path);
    ASSERT_EQ("eos://dir2/file2", atfr.getRemoteFile().path.getRaw());
    ASSERT_EQ(12345678, atfr.getRemoteFile().status.size);
    cta::objectstore::CreationLog log(atfr.getCreationLog());
    ASSERT_EQ(123, log.user.uid);
    ASSERT_EQ(456, log.user.gid);
    ASSERT_EQ("testHost", log.host);
    ASSERT_EQ(now, log.time);
    ASSERT_EQ("Comment", log.comment);
  }
}

}