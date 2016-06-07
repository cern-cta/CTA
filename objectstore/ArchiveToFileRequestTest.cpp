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
#include "EntryLog.hpp"
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
    atfr.initialize();cta::common::archiveNS::ArchiveFile af;
    af.fileId = 1232456789L;
    atfr.setArchiveFile(af);
    atfr.setRemoteFile(cta::RemotePathAndStatus(cta::RemotePath("eos://dir2/file2"), 
      cta::RemoteFileStatus(cta::common::dataStructures::UserIdentity("user0", "group0"), 0744, 12345678)));
    cta::common::dataStructures::EntryLog el;
    el.username = "user0";
    el.host = "testHost";
    el.time = now = time(NULL);
    atfr.setEntryLog(el);
    atfr.setPriority(12);
    atfr.insert();
  }
  {
    // Try to load back the Request and dump it.
    cta::objectstore::ArchiveToFileRequest atfr(tapeAddress, be);
    cta::objectstore::ScopedSharedLock atfrl(atfr);
    atfr.fetch();
    ASSERT_EQ(1232456789L, atfr.getArchiveFile().fileId);
    ASSERT_EQ("eos://dir2/file2", atfr.getRemoteFile().path.getRaw());
    ASSERT_EQ(12345678, atfr.getRemoteFile().status.size);
    cta::objectstore::EntryLog log(atfr.getCreationLog());
    ASSERT_EQ("user0", log.username);
    ASSERT_EQ("testHost", log.host);
    ASSERT_EQ(now, log.time);
  }
}

}