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

#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "tapeserver/castor/common/CastorConfiguration.hpp"
#include "tapeserver/castor/log/DummyLogger.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "common/SecurityIdentity.hpp"

#include <exception>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>

int main(int argc, char ** argv) {
  try{
    if (argc != 3)
      throw std::runtime_error("In ctaAddAdminUser: expected 2 arguments (uid, gid)");
    uid_t uid = atol(argv[1]);
    gid_t gid = atol(argv[2]);
    castor::common::CastorConfiguration &castorConf =
        castor::common::CastorConfiguration::getConfig();
    
    castor::log::DummyLogger log("ctaAddAdminUser");
    std::unique_ptr<cta::objectstore::Backend> be(
      cta::objectstore::BackendFactory::createBackend(
        castorConf.getConfEntString("TapeServer", "ObjectStoreBackendPath", &log)).release());
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    cta::OStoreDB db(*be);
    db.createAdminUser(cta::SecurityIdentity(cta::UserIdentity(getuid(), getgid()), 
                                             castor::utils::getHostName()),
        cta::UserIdentity(uid, gid), "");
  } catch (std::exception & e) {
    std::cerr << "In ctaAddAdminUser: got an exception: " << e.what() << std::endl;
  }
}
