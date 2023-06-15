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
 * This program will make sure every queue listed in the root entry does exist and 
 * will remove reference for the ones that do not. This utility was created to quickly
 * unblock tape servers after changing the ArchiveQueue schema during development.
 */

#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "AgentReference.hpp"
#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "common/Configuration.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "RootEntry.hpp"

#include <iostream>
#include <stdexcept>

int main(int argc, char** argv) {
  try {
    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-create-missing-repack-index");
    cta::log::LogContext lc(logger);
    std::unique_ptr<cta::objectstore::Backend> be;
    if (2 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], logger).release());
    }
    else if (1 == argc) {
      cta::common::Configuration m_ctaConf("/etc/cta/cta-objectstore-tools.conf");
      be = std::move(cta::objectstore::BackendFactory::createBackend(
        m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr), logger));
    }
    else {
      throw std::runtime_error("Wrong number of arguments: expected 0 or 1: [objectstoreURL]");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS&>(*be).noDeleteOnExit();
    }
    catch (std::bad_cast&) {
    }
    std::cout << "Object store path: " << be->getParams()->toURL() << std::endl;
    // Open the root entry RW
    std::cout << "Creating AgentReference for the creation of the repack index" << std::endl;
    cta::objectstore::AgentReference agr("cta-objectstore-create-missing-repack-index", logger);
    std::cout << "Creating Agent for the creation of the repack index" << std::endl;
    cta::objectstore::Agent ag(agr.getAgentAddress(), *be);
    std::cout << "Initializing agent" << std::endl;
    ag.initialize();
    std::cout << "Inserting agent" << std::endl;
    ag.insertAndRegisterSelf(lc);
    {
      cta::objectstore::RootEntry re(*be);
      cta::objectstore::ScopedExclusiveLock rel(re);
      re.fetch();
      try {
        std::cout << "Deleting already existing repack index" << std::endl;
        re.removeRepackIndexAndCommit(lc);
        std::cout << "Trying to insert repack index" << std::endl;
        std::string repackIndexAddress = re.addOrGetRepackIndexAndCommit(agr);
        std::cout << "Repack index created. Address = " << repackIndexAddress << std::endl;
      }
      catch (const cta::objectstore::RootEntry::DriveRegisterNotEmpty& ex) {
        std::cout << "Could not remove the already existing repack index, errorMsg = " << ex.getMessageValue();
        return 1;
      }
      catch (const cta::exception::Exception& e) {
        std::cout << "Unable to create repack index: " << e.getMessageValue() << std::endl;
        return 1;
      }
    }
  }
  catch (std::exception& e) {
    std::cerr << "Failed to create repack index: " << std::endl << e.what() << std::endl;
    return 1;
  }
}
