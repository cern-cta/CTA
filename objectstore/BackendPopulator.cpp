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

#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/BackendPopulator.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BackendPopulator::BackendPopulator(cta::objectstore::Backend & be): m_backend(be), m_agent(m_backend) {
  cta::objectstore::RootEntry re(m_backend);
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  m_agent.generateName("OStoreDBFactory");
  m_agent.initialize();
  cta::objectstore::CreationLog cl(cta::UserIdentity(1111, 1111), "systemhost", time(NULL), "Initial creation of the  object store structures");
  re.addOrGetAgentRegisterPointerAndCommit(m_agent,cl);
  rel.release();
  m_agent.insertAndRegisterSelf();
  rel.lock(re);
  re.addOrGetDriveRegisterPointerAndCommit(m_agent, cl);
  rel.release();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BackendPopulator::~BackendPopulator() throw() {
  cta::objectstore::ScopedExclusiveLock agl(m_agent);
  m_agent.fetch();
  m_agent.removeAndUnregisterSelf();
}

//------------------------------------------------------------------------------
// getAgent
//------------------------------------------------------------------------------
cta::objectstore::Agent & BackendPopulator::getAgent() {
  return m_agent;
}