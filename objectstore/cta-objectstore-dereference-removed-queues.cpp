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
 * This program will make sure every queue listed in the root entry does exist and 
 * will remove reference for the ones that do not. This utility was created to quickly
 * unblock tape servers after changing the ArchiveQueue schema during development.
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

#include <iostream>
#include <stdexcept>


/**
 * Get the missing Retrieve queues from the RootEntry
 * @param re the RootEntry that is considered already locked and fetched
 * @param backend the objectstore backend
 * @return the list of the addresses of the missing Retrieve queues
 */
std::list<std::string> getAllMissingRetrieveQueues(cta::objectstore::RootEntry & re, cta::objectstore::Backend & backend){
  std::list<std::string> missingRetrieveQueues;
  for(auto queueType: cta::objectstore::AllJobQueueTypes){
    std::list<cta::objectstore::RootEntry::RetrieveQueueDump> retrieveQueues = re.dumpRetrieveQueues(queueType);
    for (auto & rq: retrieveQueues){
      if (!backend.exists(rq.address)) {
        missingRetrieveQueues.emplace_back(rq.vid);
        std::cout << "The retrieve queue for vid " << rq.vid << " at address " << rq.address
              << " is missing and will be dereferenced." << std::endl;
      }
    }
  }
  return missingRetrieveQueues;
}

/**
 * Get the missing Archive queues from the RootEntry
 * @param re the RootEntry that is considered already locked and fetched
 * @param backend the objectstore backend
 * @return the list of the addresses of the missing Archive queues
 */
std::list<std::string> getAllMissingArchiveQueues(cta::objectstore::RootEntry & re, cta::objectstore::Backend & backend){
  std::list<std::string> missingArchiveQueues;
  for(auto queueType: cta::objectstore::AllJobQueueTypes){
    std::list<cta::objectstore::RootEntry::ArchiveQueueDump> archiveQueues = re.dumpArchiveQueues(queueType);
    for (auto & aq: archiveQueues){
      if (!backend.exists(aq.address)) {
        missingArchiveQueues.emplace_back(aq.tapePool);
        std::cout << "The archive queue for tape pool " << aq.tapePool << " at address " << aq.address
              << " is missing and will be dereferenced." << std::endl;
      }
    }
  }
  return missingArchiveQueues;
}


int main(int argc, char ** argv) {
  try {
    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-dereference-removed-queues");
    std::unique_ptr<cta::objectstore::Backend> be;
    if (2 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], logger).release());
    } else if (1 == argc) {
      cta::common::Configuration m_ctaConf("/etc/cta/cta-objectstore-tools.conf");
      be = std::move(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr), logger));
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 0 or 1: [objectstoreURL]");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    std::cout << "Object store path: " << be->getParams()->toURL() << std::endl;
    // Open the root entry RW
    cta::objectstore::RootEntry re(*be);
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    std::list<std::string> missingArchiveQueues, missingRetrieveQueues;
    missingArchiveQueues = getAllMissingArchiveQueues(re,*be);
    missingRetrieveQueues = getAllMissingRetrieveQueues(re,*be);
    // Actually do the job
    for (auto & tp: missingArchiveQueues) {
      re.removeMissingArchiveQueueReference(tp, cta::objectstore::JobQueueType::JobsToTransferForUser);
      std::cout << "Archive queue for tape pool " << tp << " dereferenced." << std::endl;
    }
    for (auto & vid: missingRetrieveQueues) {
      re.removeMissingRetrieveQueueReference(vid, cta::objectstore::JobQueueType::JobsToTransferForUser);
      std::cout << "Retrieve queue for vid " << vid << " dereferenced." << std::endl;
    }
    if (missingArchiveQueues.size() || missingRetrieveQueues.size()) {
      re.commit();
      std::cout << "Root entry committed." << std::endl;
    } else {
      std::cout << "Nothing to clean up from root entry." << std::endl;
    }
  } catch (std::exception & e) {
    std::cerr << "Failed to cleanup root entry: "
        << std::endl << e.what() << std::endl;
  }
}
