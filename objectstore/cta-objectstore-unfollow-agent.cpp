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

#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "Agent.hpp"
#include "RootEntry.hpp"
#include "AgentRegister.hpp"
#include <iostream>
#include <stdexcept>

int main(int argc, char ** argv) {
  try {
    std::unique_ptr<cta::objectstore::Backend> be;
    if (3 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1]).release());
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 2");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    std::cout /* << "Object store path: " << be->getParams()->toURL() 
        << " agent */<< "name=" << argv[2] << std::endl;
    cta::objectstore::Agent ag(argv[2], *be);
    cta::objectstore::ScopedExclusiveLock agl(ag);
    ag.fetch();
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
    while (depth) {
      // Simple orphan or owned by register: not a cycler.
      if ((ar.getAddressIfSet() == currentOwner) || !be->exists(ag.getOwner())) break;
      // Move to the next owner:
      depth--;
      cta::objectstore::Agent ag2(currentOwner, *be);
      cta::objectstore::ScopedSharedLock ag2l(ag2);
      ag2.fetch();
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
    ar.untrackAgent(ag.getAddressIfSet());
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