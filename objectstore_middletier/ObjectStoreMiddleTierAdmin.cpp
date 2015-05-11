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

#include "cta/Exception.hpp"
#include "ObjectStoreMiddleTierAdmin.hpp"
#include "objectstore/Backend.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/AdminUsersList.hpp"

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OStoreMiddleTierAdmin::OStoreMiddleTierAdmin(objectstore::Backend& backend):
  m_backend(backend) {
  // check that we can at least access the root entry
  objectstore::RootEntry re(m_backend);
  objectstore::ScopedSharedLock reLock(re);
  re.fetch();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OStoreMiddleTierAdmin::~OStoreMiddleTierAdmin() throw() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void OStoreMiddleTierAdmin::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  // Authz is not handled in this layer. We hence store the new admin user
  // without checks.
  objectstore::RootEntry re(m_backend);
  objectstore::ScopedSharedLock reLock(re);
  re.fetch();
  objectstore::AdminUsersList aul(re.getAdminUsersList(), m_backend);
  reLock.release();
  objectstore::ScopedExclusiveLock aulLock(aul);
  aul.fetch();
  AdminUser au(user, requester.user, comment);
  aul.add(au);
  aul.commit();
}

}
