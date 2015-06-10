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

#include "OStoreDB.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "objectstore/RootEntry.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/StorageClass.hpp"

namespace cta {
  
using namespace objectstore;

OStoreDB::OStoreDB(objectstore::Backend& be):
  m_objectStore(be) {}

OStoreDB::~OStoreDB() throw() { }

void OStoreDB::createAdminHost(const SecurityIdentity& requester,
    const std::string& hostName, const std::string& comment) {
  RootEntry re(m_objectStore);
  CreationLog cl;
  cl.uid = requester.getUser().getUid();
  cl.gid = requester.getUser().getGid();
  cl.hostname = requester.getHost();
  cl.comment = comment;
  cl.time = time(NULL);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.addAdminHost(hostName, cl);
  re.commit();
  }

  std::list<AdminHost> OStoreDB::getAdminHosts() const {
    throw exception::Exception("Not Implemented");
  }


void OStoreDB::deleteAdminHost(const SecurityIdentity& requester, 
  const std::string& hostName) {
  RootEntry re(m_objectStore);
  ScopedExclusiveLock rel(re);
  re.fetch();
  re.removeAdminHost(hostName);
  re.commit();
}

void OStoreDB::createAdminUser(const SecurityIdentity& requester, 
  const cta::UserIdentity& user, const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteAdminUser(const SecurityIdentity& requester, 
  const cta::UserIdentity& user) {
  throw exception::Exception("Not Implemented");
}

std::list<AdminUser> OStoreDB::getAdminUsers() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::assertIsAdminOnAdminHost(const SecurityIdentity& id) const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::createStorageClass(const SecurityIdentity& requester, 
  const std::string& name, const uint16_t nbCopies, const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

StorageClass OStoreDB::getStorageClass(const std::string& name) const {
  throw exception::Exception("Not Implemented");
}

std::list<StorageClass> OStoreDB::getStorageClasses() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteStorageClass(const SecurityIdentity& requester, 
  const std::string& name) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::createArchivalRoute(const SecurityIdentity& requester, 
  const std::string& storageClassName, const uint16_t copyNb, 
  const std::string& tapePoolName, const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

std::list<ArchivalRoute> 
  OStoreDB::getArchivalRoutes(const std::string& storageClassName) const {
  throw exception::Exception("Not Implemented");
}

std::list<ArchivalRoute> OStoreDB::getArchivalRoutes() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteArchivalRoute(const SecurityIdentity& requester, 
  const std::string& storageClassName, const uint16_t copyNb) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::createTapePool(const SecurityIdentity& requester, 
  const std::string& name, const uint32_t nbPartialTapes, 
  const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

std::list<TapePool> OStoreDB::getTapePools() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteTapePool(const SecurityIdentity& requester, 
  const std::string& name) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::createTape(const SecurityIdentity& requester, 
  const std::string& vid, const std::string& logicalLibraryName, 
  const std::string& tapePoolName, const uint64_t capacityInBytes, 
  const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

std::list<Tape> OStoreDB::getTapes() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteTape(const SecurityIdentity& requester, 
  const std::string& vid) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::createLogicalLibrary(const SecurityIdentity& requester,
  const std::string& name, const std::string& comment) {
  throw exception::Exception("Not Implemented");
}

std::list<LogicalLibrary> OStoreDB::getLogicalLibraries() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteLogicalLibrary(const SecurityIdentity& requester, 
  const std::string& name) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::queue(const ArchiveToFileRequest& rqst) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::queue(const ArchiveToDirRequest& rqst) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteArchiveToFileRequest(const SecurityIdentity& requester, 
  const std::string& archiveFile) {
  throw exception::Exception("Not Implemented");
}

std::map<TapePool, std::list<ArchiveToTapeCopyRequest> >
  OStoreDB::getArchiveRequests() const {
  throw exception::Exception("Not Implemented");
}

std::list<ArchiveToTapeCopyRequest>
  OStoreDB::getArchiveRequests(const std::string& tapePoolName) const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::queue(const RetrieveToFileRequest& rqst) {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::queue(const RetrieveToDirRequest& rqst) {
  throw exception::Exception("Not Implemented");
}

std::list<RetrieveFromTapeCopyRequest> OStoreDB::getRetrieveRequests(const std::string& vid) const {
  throw exception::Exception("Not Implemented");
}

std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > OStoreDB::getRetrieveRequests() const {
  throw exception::Exception("Not Implemented");
}

void OStoreDB::deleteRetrieveRequest(const SecurityIdentity& requester, 
  const std::string& remoteFile) {
  throw exception::Exception("Not Implemented");
}


    
}
