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

#include "middletier/sharedtest/MiddleTierAbstractTest.hpp"
#include "middletier/objectstore/OStoreMiddleTierAdmin.hpp"
#include "middletier/objectstore/OStoreMiddleTierUser.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/Agent.hpp"
#include "scheduler/MiddleTierAdmin.hpp"
#include "scheduler/MiddleTierUser.hpp"
#include "nameserver/MockNameServer.hpp"

namespace unitTests {
  
class InitializedVFSOStore: public cta::objectstore::BackendVFS {
public:
  InitializedVFSOStore(): cta::objectstore::BackendVFS() {
    // Create the root entry
    cta::objectstore::RootEntry re(*this);
    re.initialize();
    re.insert();
  }
};

class OStoreVfsMiddleTier: public localMiddleTier {
public:
  OStoreVfsMiddleTier(): m_vfsOStore(), m_agent(m_vfsOStore),
    m_mockNameServer(),
    m_admin(m_vfsOStore, m_agent), m_user(m_vfsOStore, m_mockNameServer) {
    m_agent.generateName("OStoreVfsMiddleTier");
  }
  virtual cta::MiddleTierAdmin & admin () { return m_admin; }
  virtual cta::MiddleTierUser & user () { return m_user; }
private:
  InitializedVFSOStore m_vfsOStore;
  cta::objectstore::Agent m_agent;
  cta::MockNameServer m_mockNameServer;
  cta::OStoreMiddleTierAdmin m_admin;
  cta::OStoreMiddleTierUser m_user;
};

class OStoreVfsMiddleTierFactory: public MiddleTierFactory {
public:
  OStoreVfsMiddleTierFactory() {
    m_localMiddleTier.reset(allocateLocalMiddleTier());
  }
  virtual localMiddleTier * allocateLocalMiddleTier() { 
    return new OStoreVfsMiddleTier; }
} g_OStoreMiddleTierFactory;

// Macro chokes on implicit casting of pointer so we have to do it ourselves
INSTANTIATE_TEST_CASE_P(MiddleTierOStore, MiddleTierAbstractTest, ::testing::Values(
    (MiddleTierFactory*)&g_OStoreMiddleTierFactory));

}
