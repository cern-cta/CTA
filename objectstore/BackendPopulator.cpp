/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "common/utils/utils.hpp"

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BackendPopulator::BackendPopulator(cta::objectstore::Backend & be,
    const std::string &agentType, const cta::log::LogContext & lc): m_backend(be), m_agentReference(agentType, lc.logger()),
    m_lc(lc) {
  cta::objectstore::RootEntry re(m_backend);
  re.fetchNoLock();
  cta::objectstore::EntryLogSerDeser cl("user0", "systemhost", time(nullptr));
  // We might have to create the agent register (but this is unlikely)
  log::LogContext lc2(lc);
  try {
    re.getAgentRegisterAddress();
  } catch (...) {
    RootEntry re2(m_backend);
    ScopedExclusiveLock rel(re2);
    re2.fetch();
    re2.addOrGetAgentRegisterPointerAndCommit(m_agentReference, cl, lc2);
  }
  Agent agent(m_agentReference.getAgentAddress(), m_backend);
  agent.initialize();
  agent.insertAndRegisterSelf(lc2);
  // Likewise, make sure the drive register is around.
  try {
    re.getDriveRegisterAddress();
  } catch (...) {
    RootEntry re2(m_backend);
    ScopedExclusiveLock rel(re2);
    re2.fetch();
    re2.addOrGetDriveRegisterPointerAndCommit(m_agentReference, cl);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BackendPopulator::~BackendPopulator() throw() {
  try {
    Agent agent(m_agentReference.getAgentAddress(), m_backend);
    cta::objectstore::ScopedExclusiveLock agl(agent);
    agent.fetch();
    if (m_leaveNonEmptyAgentBehind && !agent.isEmpty()) {
      cta::log::ScopedParamContainer params(m_lc);
      agent.setNeedsGarbageCollection();
      agent.commit();
      params.add("agentObject", agent.getAddressIfSet())
            .add("ownedObjectCount", agent.getOwnershipList().size());
      m_lc.log(log::INFO, "In BackendPopulator::~BackendPopulator(): not deleting non-empty agent object, left for garbage collection.");
      return;
    }
    agent.removeAndUnregisterSelf(m_lc);
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("errorMessage", ex.getMessageValue());
    m_lc.log(log::CRIT, "In BackendPopulator::~BackendPopulator(): error deleting agent (cta::exception::Exception). Backtrace follows.");
    m_lc.logBacktrace(log::INFO, ex.backtrace());
    // We have an exception (we should not), let's core dump.
    cta::utils::segfault();
  } catch (std::exception & ex) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add("exceptionWhat", ex.what());
    m_lc.log(log::CRIT, "In BackendPopulator::~BackendPopulator(): error deleting agent (std::exception).");
    // We have an exception (we should not), let's core dump.
    cta::utils::segfault();
  }
}

//------------------------------------------------------------------------------
// getAgent
//------------------------------------------------------------------------------
cta::objectstore::AgentReference & BackendPopulator::getAgentReference() {
  return m_agentReference;
}

//------------------------------------------------------------------------------
// leaveNonEmptyAgentsBehind
//------------------------------------------------------------------------------
void BackendPopulator::leaveNonEmptyAgentsBehind() {
  m_leaveNonEmptyAgentBehind=true;
}


}}
