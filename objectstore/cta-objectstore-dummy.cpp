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
#include "ArchiveQueueShard.hpp"
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
    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-dummy");
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
    cta::objectstore::ArchiveQueueShard aqs(*be);
    aqs.setAddress("dummy-shard");
    aqs.initialize("ArchiveQueueToTransferForUser-r_archive_1-Frontend-ctaproductionfrontend01.cern.ch-23877-20230920-11:43:11-0-1645730"); //Owner

    for (int i = 0; i < 893; i++) {
      cta::objectstore::ArchiveQueue::JobToAdd obj;
      obj.archiveRequestAddress = std::string("dummy-archive-request-ref-") + std::to_string(i);
      aqs.addJob(obj);
    }
    aqs.overrideTotalSize(5772115128);
    aqs.insert();
    return EXIT_SUCCESS;
  } catch (std::exception & e) {
    return EXIT_FAILURE;
  }
}
