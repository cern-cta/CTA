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

/**
 * This program will create a VFS backend for the object store and populate
 * it with the minimum elements (the root entry). The program will then print out
 * the path the backend store and exit
 */

#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "common/Configuration.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "RootEntry.hpp"
#include "SerializersExceptions.hpp"

#include <iostream>
#include <stdexcept>

int main(int argc, char ** argv) {
  try {
    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-unfollow-agent");
    std::string agentName;
    std::unique_ptr<cta::objectstore::Backend> be;
    if (3 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], logger).release());
      agentName = argv[2];
    } else if (2 == argc) {
      cta::common::Configuration m_ctaConf("/etc/cta/cta-objectstore-tools.conf");
      be=std::move(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr), logger));
      agentName = argv[1];
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 1 or 2: [objectstoreURL] agentName");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    std::cout /* << "Object store path: " << be->getParams()->toURL() 
        << " agent */<< "name=" << agentName << std::endl;
    if (!be->exists(agentName)) {
      // Agent does not exist: remove from registry.
      cta::objectstore::RootEntry re (*be);
      cta::objectstore::ScopedSharedLock rel(re);
      re.fetch();
      cta::objectstore::AgentRegister ar(re.getAgentRegisterAddress(), *be);
      rel.release();
      cta::objectstore::ScopedExclusiveLock arl(ar);
      ar.fetch();
      ar.removeAgent(agentName);
      ar.commit();
      std::cout << "De-listed a non-existing agent." << std::endl;
      exit (EXIT_SUCCESS);
    }
    cta::objectstore::Agent ag(agentName, *be);
    cta::objectstore::ScopedExclusiveLock agl(ag);
    try {
      ag.fetch();
    } catch (cta::objectstore::ObjectOpsBase::WrongType &) {
      // Failure to parse an object might come for a zero size one. If this is the case, remove it
      if (be->read(ag.getAddressIfSet()).empty()) {
        be->remove(ag.getAddressIfSet());
        std::cout << "Agent object was empty. Removed it and exiting." << std::endl;
        exit (EXIT_SUCCESS);
      }
    }
    // Add the agent to the list of untracked agents
    cta::objectstore::RootEntry re (*be);
    cta::objectstore::ScopedSharedLock rel(re);
    re.fetch();
    cta::objectstore::AgentRegister ar(re.getAgentRegisterAddress(), *be);
    rel.release();
    // Check that the agent is indeed orphaned (owned by a non-existing owner).
    // Also check that he is not an active member of a cyclic ownership. To check we follow the 
    // ownership chain for a limited depth. If the chain comes back to this agent, we count it
    // as a active cycler.
    size_t depth = 50;
    std::string currentOwner = ag.getOwner();
    std::string cuttrentlyConsideredAgent = ag.getAddressIfSet();
    while (depth) {
      // Simple orphan or owned by register: not a cycler.
      if (ar.getAddressIfSet() == currentOwner) {
        std::cout << "This agent is owner by the agent register." << std::endl;
        break;
      } else if (!be->exists(currentOwner)) {
        std::cout << "This agent is owned by a broken chain of ownership: " << cuttrentlyConsideredAgent << "is orphaned." << std::endl; 
        break;
      }
      // Move to the next owner:
      depth--;
      cta::objectstore::Agent ag2(currentOwner, *be);
      cta::objectstore::ScopedSharedLock ag2l(ag2);
      ag2.fetch();
      cuttrentlyConsideredAgent = currentOwner;
      currentOwner = ag2.getOwner();
      if (currentOwner == ag.getAddressIfSet()) {
        std::cout << "This agent is a cycler." << std::endl;
        goto cycler;
      }
    }
    if ((ar.getAddressIfSet() != ag.getOwner()) && be->exists(ag.getOwner())) {
      std::stringstream err;
      err << "Agent not orphaned: owner object exists: " << ag.getOwner();
      throw std::runtime_error(err.str().c_str());
    }
cycler:
    cta::objectstore::ScopedExclusiveLock arl(ar);
    ar.fetch();
    try {
      ar.untrackAgent(ag.getAddressIfSet());
    } catch (cta::objectstore::serializers::NotFound &) {
      std::cout << "Agent was not known to the agent register. Re-registering it." << std::endl;
      ar.addAgent(ag.getAddressIfSet());
    }
    ar.commit();
    arl.release();
    ag.setOwner(ar.getAddressIfSet());
    ag.commit();
    agl.release();
    std::cout << "Agent is now listed as untracked." << std::endl;
  } catch (std::exception & e) {
    std::cerr << "Failed to untrack object: "
        << std::endl << e.what() << std::endl;
  }
}
