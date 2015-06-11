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
#include "common/Utils.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/ArchiveToDirRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveFromTapeCopyRequest.hpp"
#include "scheduler/RetrieveToDirRequest.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"
#include "scheduler/UserIdentity.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler(NameServer &ns, SchedulerDatabase &db):
  m_ns(ns),
  m_db(db) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Scheduler::~Scheduler() throw() {
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchiveToTapeCopyRequest> >
  cta::Scheduler::getArchiveRequests(const SecurityIdentity &requester) const {
  return m_db.getArchiveRequests();
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::list<cta::ArchiveToTapeCopyRequest> cta::Scheduler::getArchiveRequests(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  return m_db.getArchiveRequests(tapePoolName);
}

//------------------------------------------------------------------------------
// deleteArchiveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRequest(
  const SecurityIdentity &requester,
  const std::string &archiveFile) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteArchiveRequest(requester, archiveFile);
  m_ns.deleteFile(requester, archiveFile);
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrieveFromTapeCopyRequest> > cta::
  Scheduler::getRetrieveRequests(const SecurityIdentity &requester) const {
  return m_db.getRetrieveRequests();
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveFromTapeCopyRequest> cta::Scheduler::getRetrieveRequests(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  return m_db.getRetrieveRequests(vid);
}
  
//------------------------------------------------------------------------------
// deleteRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::deleteRetrieveRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteRetrieveRequest(requester, remoteFile);
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// createAdminUserWithoutAuthorizingRequester
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminUserWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  m_db.createAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteAdminUser(requester, user);
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::Scheduler::getAdminUsers(const SecurityIdentity
  &requester) const {
  m_db.assertIsAdminOnAdminHost(requester);
  return m_db.getAdminUsers();
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// createAdminHostWithoutAuthorizingRequester
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHostWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  m_db.createAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteAdminHost(requester, hostName);
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::Scheduler::getAdminHosts(const SecurityIdentity
  &requester) const {
  return m_db.getAdminHosts();
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createStorageClass(requester, name, nbCopies, comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_ns.assertStorageClassIsNotInUse(requester, name, "/");
  m_db.deleteStorageClass(requester, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::Scheduler::getStorageClasses(
  const SecurityIdentity &requester) const {
  return m_db.getStorageClasses();
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createTapePool(requester, name, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::Scheduler::getTapePools(
  const SecurityIdentity &requester) const {
  return m_db.getTapePools();
}

//------------------------------------------------------------------------------
// createArchivalRoute
//------------------------------------------------------------------------------
void cta::Scheduler::createArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createArchivalRoute(requester, storageClassName, copyNb, tapePoolName,
    comment);
}

//------------------------------------------------------------------------------
// deleteArchivalRoute
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteArchivalRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::Scheduler::getArchivalRoutes(
  const SecurityIdentity &requester) const {
  return m_db.getArchivalRoutes();
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::Scheduler::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return m_db.getLogicalLibraries();
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::Scheduler::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createTape(requester, vid, logicalLibraryName, tapePoolName,
    capacityInBytes, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::Scheduler::getTapes(
  const SecurityIdentity &requester) const {
  return m_db.getTapes();
}

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::Scheduler::createDir(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t mode) {
  m_ns.createDir(requester, path, mode);
}

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::Scheduler::deleteDir(
  const SecurityIdentity &requester,
  const std::string &path) {
  m_ns.deleteDir(requester, path);
}

//------------------------------------------------------------------------------
// regularFileExists
//------------------------------------------------------------------------------
bool cta::Scheduler::regularFileExists(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.regularFileExists(requester, path);
}

//------------------------------------------------------------------------------
// dirExists
//------------------------------------------------------------------------------
bool cta::Scheduler::dirExists(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.dirExists(requester, path);
}
  
//------------------------------------------------------------------------------
// getVidOfFile
//------------------------------------------------------------------------------
std::string cta::Scheduler::getVidOfFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t copyNb) const {
  return m_ns.getVidOfFile(requester, path, copyNb);
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::DirIterator cta::Scheduler::getDirContents(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.getDirContents(requester, path);
}

//------------------------------------------------------------------------------
// statDirEntry
//------------------------------------------------------------------------------
cta::DirEntry cta::Scheduler::statDirEntry(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.statDirEntry(requester, path);
}

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::setDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path,
  const std::string &storageClassName) {
  m_ns.setDirStorageClass(requester, path, storageClassName);
}

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::clearDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) {
  m_ns.clearDirStorageClass(requester, path);
}
  
//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::Scheduler::getDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.getDirStorageClass(requester, path);
}

//------------------------------------------------------------------------------
// queueArchiveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueArchiveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &remoteFiles,
  const std::string &archiveFileOrDir) {

  const bool archiveToDir = m_ns.dirExists(requester, archiveFileOrDir);
  if(archiveToDir) {
    const std::string &archiveDir = archiveFileOrDir;
    if(remoteFiles.empty()) {
      std::ostringstream message;
      message << "Invalid archive to directory request:"
        " The must be at least one remote file:"
        " archiveDir=" << archiveDir;
      throw exception::Exception(message.str());
    }

    queueArchiveToDirRequest(requester, remoteFiles, archiveDir);

  // Else archive to a single file
  } else {
    const std::string &archiveFile = archiveFileOrDir;
    if(1 != remoteFiles.size()) {
      std::ostringstream message;
      message << "Invalid archive to file request:"
        " The must be one and only one remote file:"
        " actual=" << remoteFiles.size() << " archiveFile=" << archiveFile;
      throw exception::Exception(message.str());
    }

    queueArchiveToFileRequest(requester, remoteFiles.front(), archiveFile);
  }
}

//------------------------------------------------------------------------------
// queueArchiveToDirRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueArchiveToDirRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &remoteFiles,
  const std::string &archiveDir) {

  const uint64_t priority = 0; // TO BE DONE

  const auto storageClassName = m_ns.getDirStorageClass(requester, archiveDir);
  const auto storageClass = m_db.getStorageClass(storageClassName);
  assertStorageClassHasAtLeastOneCopy(storageClass);
  const auto archiveToFileRequests = createArchiveToFileRequests(requester,
    remoteFiles, archiveDir, priority);

  m_db.queue(ArchiveToDirRequest(archiveDir, archiveToFileRequests, priority,
    requester));

  for(auto itor = archiveToFileRequests.cbegin(); itor !=
    archiveToFileRequests.cend(); itor++) {
    const std::string &archiveFile = itor->getArchiveFile();
    m_ns.createFile(requester, archiveFile, 0666);
    m_db.fileEntryCreatedInNS(archiveFile);
  }
}

//------------------------------------------------------------------------------
// assertStorageClassHasAtLeastOneCopy
//------------------------------------------------------------------------------
void cta::Scheduler::assertStorageClassHasAtLeastOneCopy(
  const StorageClass &storageClass) const {
  if(0 == storageClass.getNbCopies()) {
    std::ostringstream message;
    message << "Storage class " << storageClass.getName() <<
      " has a tape copy count of 0";
    throw(exception::Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// createArchiveToFileRequests
//------------------------------------------------------------------------------
std::list<cta::ArchiveToFileRequest> cta::Scheduler::
  createArchiveToFileRequests(
  const SecurityIdentity &requester,
  const std::list<std::string> &remoteFiles,
  const std::string &archiveDir,
  const uint64_t priority) {
  const bool archiveDirEndsWithASlash = Utils::endsWith(archiveDir, '/');
  std::list<ArchiveToFileRequest> archiveToFileRequests;

  for(auto itor = remoteFiles.cbegin(); itor != remoteFiles.cend(); itor++) {
    const auto remoteFile = *itor;
    const auto remoteFileName = Utils::getEnclosedName(remoteFile);
    const std::string archiveFile = archiveDirEndsWithASlash ?
      archiveDir + remoteFileName : archiveDir + '/' + remoteFileName;
    archiveToFileRequests.push_back(createArchiveToFileRequest(requester,
      remoteFile, archiveFile, priority));
  }  

  return archiveToFileRequests;
}

//------------------------------------------------------------------------------
// queueArchiveToFileRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueArchiveToFileRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile,
  const std::string &archiveFile) {

  const uint64_t priority = 0; // TO BE DONE
  const ArchiveToFileRequest rqst = createArchiveToFileRequest(requester,
    remoteFile, archiveFile, priority);

  m_db.queue(rqst);
  try {
    m_ns.createFile(requester, archiveFile, 0666);
  } catch(std::exception &nsEx) {
    // Try our best to cleanup the scheduler database.  The garbage collection
    // logic will try again if we fail.
    try {
      m_db.deleteArchiveRequest(requester, archiveFile);
    } catch(...) {
    }

    // Whether or not we were able to clean up the scheduler database, the real
    // problem here was the failure to create an entry in the NS
    throw nsEx;
  }

  m_db.fileEntryCreatedInNS(archiveFile);
}

//------------------------------------------------------------------------------
// createArchiveToFileRequest
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest cta::Scheduler::createArchiveToFileRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile,
  const std::string &archiveFile,
  const uint64_t priority) const {

  const std::string enclosingPath = Utils::getEnclosingPath(archiveFile);
  const std::string storageClassName = m_ns.getDirStorageClass(requester,
     enclosingPath);
  const StorageClass storageClass = m_db.getStorageClass(storageClassName);
  assertStorageClassHasAtLeastOneCopy(storageClass);
  const auto routes = m_db.getArchivalRoutes(storageClassName);
  const auto copyNbToPoolMap = createCopyNbToPoolMap(routes);

  return ArchiveToFileRequest(
    remoteFile,
    archiveFile,
    copyNbToPoolMap,
    priority,
    requester);
}

//------------------------------------------------------------------------------
// createCopyNbToPoolMap
//------------------------------------------------------------------------------
std::map<uint16_t, std::string> cta::Scheduler::createCopyNbToPoolMap(
  const std::list<ArchivalRoute> &routes) const {
  std::map<uint16_t, std::string> copyNbToPoolMap;
    for(auto itor = routes.begin(); itor != routes.end(); itor++) {
    const ArchivalRoute &route = *itor;
    copyNbToPoolMap[route.getCopyNb()] = route.getTapePoolName();
  }
  return copyNbToPoolMap;
}

//------------------------------------------------------------------------------
// queueRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueRetrieveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &archiveFiles,
  const std::string &remoteFileOrDir) {
/*
  const bool retreiveToDir = m_ns.dirExists(requester, archiveFileOrDir);
  if(archiveToDir) {
    const std::string &archiveDir = archiveFileOrDir;
    if(remoteFiles.empty()) {
      std::ostringstream message;
      message << "Invalid archive to directory request:"
        " The must be at least one remote file:"
        " archiveDir=" << archiveDir;
      throw exception::Exception(message.str());
    }

    queueArchiveToDirRequest(requester, remoteFiles, archiveDir);

  // Else archive to a single file
  } else {
    const std::string &archiveFile = archiveFileOrDir;
    if(1 != remoteFiles.size()) {
      std::ostringstream message;
      message << "Invalid archive to file request:"
        " The must be one and only one remote file:"
        " actual=" << remoteFiles.size() << " archiveFile=" << archiveFile;
      throw exception::Exception(message.str());
    }

    queueArchiveToFileRequest(requester, remoteFiles.front(), archiveFile);
  }
*/
}
