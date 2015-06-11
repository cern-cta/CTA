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

#include "OStoreDBFactory.hpp"
#include "OStoreDB.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/Agent.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::
OStoreDBFactory::OStoreDBFactory(objectstore::Backend& backend):
  m_backend(backend), m_initDone(false) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::OStoreDBFactory::~OStoreDBFactory() throw() {
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<cta::SchedulerDatabase> cta::OStoreDBFactory::
  create() const {
  // Lazy initialization. Prevents issues with dependancies on protocol buffers'
  // own static initialization
  if (!m_initDone) {
    // We need to populate the root entry before using.
    objectstore::RootEntry re(m_backend);
    re.initialize();
    re.insert();
    objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    objectstore::Agent ag(m_backend);
    ag.generateName("OStoreDBFactory");
    ag.initialize();
    objectstore::CreationLog cl(1111, 1111, "systemhost", 
      time(NULL), "Initial creation of the  object store structures");
    re.addOrGetAgentRegisterPointerAndCommit(ag,cl);
    rel.release();
    ag.insertAndRegisterSelf();
    rel.lock(re);
    re.addOrGetDriveRegisterPointerAndCommit(ag, cl);
    rel.release();
    objectstore::ScopedExclusiveLock agl(ag);
    ag.removeAndUnregisterSelf();
    m_initDone = true;
  }
  return std::unique_ptr<SchedulerDatabase>(new OStoreDB(m_backend));
}