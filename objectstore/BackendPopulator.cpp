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

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BackendPopulator::BackendPopulator(cta::objectstore::Backend & be): m_backend(be), m_agentReference("OStoreDBFactory") {
  cta::objectstore::RootEntry re(m_backend);
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  Agent agent(m_agentReference.getAgentAddress(), m_backend);
  agent.initialize();
  cta::objectstore::EntryLogSerDeser cl("user0", "systemhost", time(NULL));
  re.addOrGetAgentRegisterPointerAndCommit(m_agentReference, cl);
  rel.release();
  agent.insertAndRegisterSelf();
  rel.lock(re);
  re.addOrGetDriveRegisterPointerAndCommit(m_agentReference, cl);
  rel.release();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BackendPopulator::~BackendPopulator() throw() {
  Agent agent(m_agentReference.getAgentAddress(), m_backend);
  cta::objectstore::ScopedExclusiveLock agl(agent);
  agent.fetch();
  agent.removeAndUnregisterSelf();
}

//------------------------------------------------------------------------------
// getAgent
//------------------------------------------------------------------------------
cta::objectstore::AgentReference & BackendPopulator::getAgentReference() {
  return m_agentReference;
}

}}