/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "RootEntry.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace {

void initializeObjectStore(cta::objectstore::Backend& backend, cta::log::Logger& logger) {
  cta::log::LogContext lc(logger);
  cta::objectstore::RootEntry rootEntry(backend);
  rootEntry.initialize();
  rootEntry.insert();

  cta::objectstore::ScopedExclusiveLock rootEntryLock(rootEntry);
  rootEntry.fetch();

  cta::objectstore::AgentReference agentReference("cta-objectstore-reset", logger);
  cta::objectstore::Agent agent(agentReference.getAgentAddress(), backend);
  agent.initialize();

  cta::objectstore::EntryLogSerDeser entryLog("user0",
                                              "systemhost",
                                              std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  rootEntry.addOrGetAgentRegisterPointerAndCommit(agentReference, entryLog, lc);
  rootEntryLock.release();
  agent.insertAndRegisterSelf(lc);

  rootEntryLock.lock(rootEntry);
  rootEntry.fetch();
  rootEntry.addOrGetDriveRegisterPointerAndCommit(agentReference, entryLog);
  rootEntry.addOrGetSchedulerGlobalLockAndCommit(agentReference, entryLog);

  {
    cta::objectstore::ScopedExclusiveLock agentLock(agent);
    agent.fetch();
    agent.removeAndUnregisterSelf(lc);
  }

  rootEntryLock.release();
}

}  // namespace

int main(int argc, char** argv) {
  std::unique_ptr<cta::objectstore::Backend> backend;

  try {
    if (argc != 2) {
      throw std::runtime_error("Wrong number of arguments: expected objectstoreURL");
    }

    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-reset");
    backend = cta::objectstore::BackendFactory::createBackend(argv[1], logger);

    // A VFS backend created from an explicit path is persistent by default. Keep
    // this explicit in case that implementation detail changes.
    try {
      dynamic_cast<cta::objectstore::BackendVFS&>(*backend).noDeleteOnExit();
    } catch (std::bad_cast&) {}

    const auto objects = backend->list();
    for (const auto& object : objects) {
      backend->remove(object);
    }

    std::cout << "Removed " << objects.size() << " objects from " << argv[1] << std::endl;
    initializeObjectStore(*backend, logger);
    std::cout << "Reset object store: " << backend->getParams()->toURL() << std::endl;
    return EXIT_SUCCESS;
  } catch (const std::exception& ex) {
    std::cerr << "Failed to reset " << ((backend != nullptr) ? backend->typeName() : "no-backend") << " objectstore"
              << std::endl
              << ex.what() << std::endl;
    return EXIT_FAILURE;
  }
}
