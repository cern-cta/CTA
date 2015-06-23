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
#include "Tape.hpp"
#include "BackendVFS.hpp"
#include "Agent.hpp"

namespace unitTests {
  
TEST(Tape, BasicAccess) {
  cta::objectstore::BackendVFS be;
  cta::objectstore::Agent agent(be);
  agent.generateName("unitTest");
  std::string tapeAddress = agent.nextId("Tape");
  { 
    // Try to create the tape entry
    cta::objectstore::Tape t(tapeAddress, be);
    t.initialize("V12345");
    t.insert();
  }
  {
    // Try to read back and dump the tape
    cta::objectstore::Tape t(tapeAddress, be);
    ASSERT_THROW(t.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedSharedLock lock(t);
    ASSERT_NO_THROW(t.fetch());
    t.dump();
  }
  // Delete the root entry
  cta::objectstore::Tape t(tapeAddress, be);
  cta::objectstore::ScopedExclusiveLock lock(t);
  t.fetch();
  t.removeIfEmpty();
  ASSERT_EQ(false, t.exists());
}
}