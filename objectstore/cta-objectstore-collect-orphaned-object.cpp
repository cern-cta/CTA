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
 * This program takes the address of an object as second parameter. If the object is orphaned,
 * it will be garbage collected.
 */

#include <bits/unique_ptr.h>

#include <iostream>
#include <stdexcept>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/Configuration.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/utils.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentReference.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/RetrieveRequest.hpp"
#include "objectstore/RootEntry.hpp"
#include "rdbms/Login.hpp"

int main(int argc, char ** argv) {
  try {
    std::unique_ptr<cta::objectstore::Backend> be;
    std::unique_ptr<cta::catalogue::Catalogue> catalogue;
    cta::log::StringLogger sl(cta::utils::getShortHostname(), "cta-objectstore-collect-orphaned", cta::log::DEBUG);
    cta::log::LogContext lc(sl);
    std::string objectName;
    if (4 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], sl).release());
      const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(argv[2]);
      const uint64_t nbConns = 1;
      const uint64_t nbArchiveFileListingConns = 0;
      auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(sl, catalogueLogin, nbConns,
        nbArchiveFileListingConns);
      catalogue=catalogueFactory->create();
      objectName = argv[3];
    } else if (2 == argc) {
      cta::common::Configuration m_ctaConf("/etc/cta/cta-objectstore-tools.conf");
      be = std::move(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr), sl));
      const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile("/etc/cta/cta-catalogue.conf");
      const uint64_t nbConns = 1;
      const uint64_t nbArchiveFileListingConns = 0;
      auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(sl, catalogueLogin, nbConns,
        nbArchiveFileListingConns);
      catalogue = catalogueFactory->create();
      objectName = argv[1];
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 1 or 3: [objectstoreURL catalogueLoginFile] objectname");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    std::cout << "Object store path: " << be->getParams()->toURL() << std::endl;
    // Try and open the object.
    cta::objectstore::GenericObject go(objectName, *be);
    cta::objectstore::ScopedExclusiveLock gol(go);
    std::cout << "Object address: " << go.getAddressIfSet() << std::endl;
    go.fetch();
    // Create an AgentReference in case we need it
    cta::objectstore::AgentReference agr("cta-objectstore-collect-orphaned-object", sl);
    cta::objectstore::Agent ag(agr.getAgentAddress(), *be);
    ag.initialize();
    ag.insertAndRegisterSelf(lc);
    switch (go.type()) {
    case cta::objectstore::serializers::ObjectType::ArchiveRequest_t:
    {
      // Reopen the object as an ArchiveRequest
      std::cout << "The object is an ArchiveRequest" << std::endl;
      gol.release();
      bool someGcDone=false;
    gcpass:
      cta::objectstore::ArchiveRequest ar(objectName, *be);
      cta::objectstore::ScopedExclusiveLock arl(ar);
      ar.fetch();
      for (auto & j: ar.dumpJobs()) {
        if (!be->exists(j.owner)) {
          std::cout << "Owner " << j.owner << " for job " <<  j.copyNb << " does not exist." << std::endl;
          ar.garbageCollect(j.owner, agr, lc, *catalogue);
          someGcDone=true;
          goto gcpass;
        }
      }
      if (someGcDone) {
        ar.fetch();
        for (auto & j: ar.dumpJobs()) {
          std::cout << "New owner for job " << j.copyNb << " is " << j.owner << std::endl;
        }
      } else {
        std::cout << "No job was orphaned." << std::endl;
      }
      break;
    }
    case cta::objectstore::serializers::ObjectType::RetrieveRequest_t:
    {
      // Reopen the object as an ArchiveRequest
      std::cout << "The object is an RetrieveRequest" << std::endl;
      gol.release();
      cta::objectstore::RetrieveRequest rr(objectName, *be);
      cta::objectstore::ScopedExclusiveLock rrl(rr);
      rr.fetch();
      if (!be->exists(rr.getOwner())) {
        std::cout << "Owner " << rr.getOwner() << " does not exist." << std::endl;
        rr.garbageCollect(rr.getOwner(), agr, lc, *catalogue);
        rr.fetch();
        std::cout << "New owner for request is " << rr.getOwner() << std::endl;
      } else {
        std::cout << "No request was not orphaned." << std::endl;
      }
      break;
    }
    default:
      std::cout << "Object type: " << go.type() << " not supported for this operation" << std::endl;
      break;
    }
    cta::objectstore::ScopedExclusiveLock agl(ag);
    ag.removeAndUnregisterSelf(lc);
  } catch (std::exception & e) {
    std::cerr << "Failed to garbage collect object: "
        << std::endl << e.what() << std::endl;
  }
}
