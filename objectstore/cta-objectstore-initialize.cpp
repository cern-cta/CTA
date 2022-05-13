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

/**
 * This program will create a VFS backend for the object store and populate
 * it with the minimum elements (the root entry). The program will then print out
 * the path the backend store and exit
 */

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "BackendVFS.hpp"
#include "BackendFactory.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "RootEntry.hpp"

#include <iostream>
#include <stdexcept>

int main(int argc, char ** argv) {
  std::unique_ptr<cta::objectstore::Backend> be;
  try {
    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-initialize");
    cta::log::LogContext lc(logger);
    if (1 == argc) {
      be.reset(new cta::objectstore::BackendVFS);
    } else if (2 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], logger).release());
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 0 or 1");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    cta::objectstore::RootEntry re(*be);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    cta::objectstore::AgentReference agr("cta-objectstore-initialize", logger);
    cta::objectstore::Agent ag(agr.getAgentAddress(), *be);
    ag.initialize();
    cta::objectstore::EntryLogSerDeser el("user0", "systemhost", time(nullptr));
    re.addOrGetAgentRegisterPointerAndCommit(agr,el, lc);
    rel.release();
    ag.insertAndRegisterSelf(lc);
    rel.lock(re);
    re.fetch();
    re.addOrGetDriveRegisterPointerAndCommit(agr, el);
    re.addOrGetSchedulerGlobalLockAndCommit(agr,el);
    {
      cta::objectstore::ScopedExclusiveLock agentLock(ag);
      ag.fetch();
      ag.removeAndUnregisterSelf(lc);
    }
    rel.release();
    std::cout << "New object store path: " << be->getParams()->toURL() << std::endl;
    return EXIT_SUCCESS;
  } catch (std::exception & e) {
    std::cerr << "Failed to initialise the root entry in a new " << ((be != nullptr) ? be->typeName() : "no-backend") << " objectstore"
        << std::endl << e.what() << std::endl;
    return EXIT_FAILURE;
  }
}
