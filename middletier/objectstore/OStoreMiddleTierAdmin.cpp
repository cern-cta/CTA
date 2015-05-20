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

#include "common/exception/Exception.hpp"
#include "middletier/objectstore/OStoreMiddleTierAdmin.hpp"
#include "objectstore/Backend.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/AdminUsersList.hpp"

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OStoreMiddleTierAdmin::OStoreMiddleTierAdmin(objectstore::Backend& backend,
  objectstore::Agent & agent):
  m_backend(backend), m_agent(agent) {
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

void OStoreMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity& requester, 
  const UserIdentity& user) {
  // Authz is not handled in this layer. We hence store the new admin user
  // without checks.
  objectstore::RootEntry re(m_backend);
  objectstore::ScopedSharedLock reLock(re);
  re.fetch();
  objectstore::AdminUsersList aul(re.getAdminUsersList(), m_backend);
  reLock.release();
  objectstore::ScopedExclusiveLock aulLock(aul);
  aul.fetch();
  aul.remove(user.getUid(), user.getGid());
  aul.commit();
  }

std::list<AdminUser> OStoreMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity& requester) const {
  std::list<AdminUser> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createAdminHost(
  const SecurityIdentity& requester,
  const std::string& hostName,
  const std::string& comment) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity& requester,
  const std::string& hostName) {
  throw cta::exception::Exception("TODO");
}

std::list<AdminHost> OStoreMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
  std::list<AdminHost> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment) {
  // Get the root entry
  objectstore::RootEntry re(m_backend);
  {
    objectstore::ScopedSharedLock sl(re);
    re.fetch();
  }
  // Get the storage class list
  re.allocateOrGetStorageClassList(m_agent);
}

void OStoreMiddleTierAdmin::deleteStorageClass(
  const SecurityIdentity& requester,
  const std::string& name) { 
  throw cta::exception::Exception("TODO");
}

std::list<StorageClass> OStoreMiddleTierAdmin::getStorageClasses(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
  std::list<StorageClass> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createTapePool(
  const SecurityIdentity& requester,
  const std::string& name,
  const uint32_t nbPartialTapes,
  const std::string& comment) {
}


void OStoreMiddleTierAdmin::deleteTapePool(
  const SecurityIdentity& requester,
  const std::string& name) {
  throw cta::exception::Exception("TODO");
}

std::list<TapePool> OStoreMiddleTierAdmin::getTapePools(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
  std::list<TapePool> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierAdmin::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  throw cta::exception::Exception("TODO");
}

std::list<ArchivalRoute> OStoreMiddleTierAdmin::getArchivalRoutes(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
  std::list<ArchivalRoute> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createLogicalLibrary(
  const SecurityIdentity& requester,
  const std::string& name,
  const std::string& comment) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity& requester,
  const std::string& name) {
  throw cta::exception::Exception("TODO");
}

std::list<LogicalLibrary> OStoreMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity& requester) const {
  throw cta::exception::Exception("TODO");
  std::list<LogicalLibrary> ret;
  return ret;
}

void OStoreMiddleTierAdmin::createTape(
  const SecurityIdentity& requester,
  const std::string& vid,
  const std::string& logicalLibraryName,
  const std::string& tapePoolName, 
  const uint64_t capacityInBytes, 
  const std::string& comment) {
  throw cta::exception::Exception("TODO");
}

void OStoreMiddleTierAdmin::deleteTape(
  const SecurityIdentity& requester,
  const std::string& vid) {
}

std::list<Tape> OStoreMiddleTierAdmin::getTapes(
  const SecurityIdentity& requester) const {
  std::list<Tape> ret;
  return ret;
}

}
