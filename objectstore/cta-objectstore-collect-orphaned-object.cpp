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
 * This program takes the address of an object as second parameter. If the object is orphaned,
 * it will be garbage collected.
 */

#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "AgentReference.hpp"
#include "Agent.hpp"
#include "RootEntry.hpp"
#include "ArchiveRequest.hpp"
#include "RetrieveRequest.hpp"
#include "GenericObject.hpp"
#include "common/log/StringLogger.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include <iostream>
#include <stdexcept>
#include <bits/unique_ptr.h>

int main(int argc, char ** argv) {
  try {
    std::unique_ptr<cta::objectstore::Backend> be;
    std::unique_ptr<cta::catalogue::Catalogue> catalogue;
    cta::log::StringLogger sl("cta-objectstore-collect-orphaned", cta::log::DEBUG);
    cta::log::LogContext lc(sl);
    if (4 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1]).release());
      const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(argv[2]);
      const uint64_t nbConns = 1;
      const uint64_t nbArchiveFileListingConns = 0;
      catalogue=std::move(cta::catalogue::CatalogueFactory::create(sl, catalogueLogin, nbConns, nbArchiveFileListingConns));
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 3: <objectstoreURL> <catalogue login file> <objectname>");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    std::cout << "Object store path: " << be->getParams()->toURL() << std::endl;
    // Try and open the object.
    cta::objectstore::GenericObject go(argv[3], *be);
    cta::objectstore::ScopedExclusiveLock gol(go);
    std::cout << "Object address: " << go.getAddressIfSet() << std::endl;
    go.fetch();
    // Create an AgentReference in case we need it
    cta::objectstore::AgentReference agr("cta-objectstore-collect-orphaned-object");
    cta::objectstore::Agent ag(agr.getAgentAddress(), *be);
    ag.initialize();
    ag.insertAndRegisterSelf();
    switch (go.type()) {
    case cta::objectstore::serializers::ObjectType::ArchiveRequest_t:
    {
      // Reopen the object as an ArchiveRequest
      std::cout << "The object is an ArchiveRequest" << std::endl;
      gol.release();
      bool someGcDone=false;
    gcpass:
      cta::objectstore::ArchiveRequest ar(argv[3], *be);
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
      cta::objectstore::RetrieveRequest rr(argv[3], *be);
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
    ag.removeAndUnregisterSelf();
  } catch (std::exception & e) {
    std::cerr << "Failed to garbage collect object: "
        << std::endl << e.what() << std::endl;
  }
}