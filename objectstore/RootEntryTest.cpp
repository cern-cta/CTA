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
#include "BackendVFS.hpp"
#include "utils/exception/Exception.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"

namespace unitTests {

TEST(RootEntry, BasicAccess) {
  cta::objectstore::BackendVFS be;
  { 
    // Try to create the root entry
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
  }
  {
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    ASSERT_THROW(re.fetch(), cta::exception::Exception);
    cta::objectstore::ScopedSharedLock lock(re);
    ASSERT_NO_THROW(re.fetch());
    std::cout << re.dump();
  }
  {
    // Try to allocate the agent register
    // Try to read back and dump the root entry
    cta::objectstore::RootEntry re(be);
    cta::objectstore::ScopedExclusiveLock lock (re);
    // Create an agent
    cta::objectstore::Agent agent(be);
    agent.generateName("unitTest");
    re.fetch();
    re.allocateOrGetAgentRegister(agent);
    ASSERT_NO_THROW(re.getAgentRegister());
    re.commit();
    //agent.registerSelf();
  }
  // Delete the root entry
  cta::objectstore::RootEntry re(be);
  cta::objectstore::ScopedExclusiveLock lock(re);
  re.remove();
  ASSERT_EQ(false, re.exists());
}

}
