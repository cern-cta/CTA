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

namespace unitTests {
  
TEST(ObjectStore, ArchiveToFileRequestBasicAccess) {
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTest");
  std::string tapeAddress = agent.nextId("ArchiveToFileRequest");
  {
    // Try to create the ArchiveToFileRequest entry
    cta::objectstore::ArchiveToFileRequest atfr(tapeAddress, be);
    atfr.initialize();
    atfr.setArchiveFile("cta://dir/file");
    atfr.setRemoteFile("eos://dir2/file2");
    atfr.setSize(12345678);
    atfr.setLog(cta::CreationLog(cta::UserIdentity(123,456),"testHost", 
      time(NULL), "Comment"));
    atfr.setPriority(12);
    atfr.insert();
  }
  {
    // Try to load back the Request and dump it.
    cta::objectstore::ArchiveToFileRequest atfr(tapeAddress, be);
    cta::objectstore::ScopedSharedLock atfrl(atfr);
    atfr.fetch();
    ASSERT_EQ("cta://dir/file", atfr.getARchiveFile());
  }
}

}