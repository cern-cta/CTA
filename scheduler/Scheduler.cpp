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

#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/archiveNS/Tape.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "common/UserIdentity.hpp"
#include "common/Utils.hpp"
#include "common/SecurityIdentity.hpp"
#include "common/TapePool.hpp"
#include "common/CreationLog.hpp"
#include "nameserver/NameServer.hpp"
#include "remotens/RemoteNS.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/ArchiveToDirRequest.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveToDirRequest.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>

//------------------------------------------------------------------------------
// TransferFailureToStr
//------------------------------------------------------------------------------
const char *cta::Scheduler::TransferFailureToStr(
  const TapeJobFailure enumValue) throw() {
  switch(enumValue) {
  case JOBFAILURE_NONE         : return "NONE";
  case JOBFAILURE_TAPEDRIVE    : return "TAPE DRIVE";
  case JOBFAILURE_TAPELIBRARY  : return "TAPE LIBRARY";
  case JOBFAILURE_REMOTESTORAGE: return "REMOTE STORAGE";
  default                           : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler(NameServer &ns,
  SchedulerDatabase &db,
  RemoteNS &remoteNS):
  m_ns(ns),
  m_db(db),
  m_remoteNS(remoteNS) {
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
  std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCancelation>
    reqCancelation(
      m_db.markArchiveRequestForDeletion(requester, archiveFile));
  m_ns.deleteFile(requester, archiveFile);
  reqCancelation->complete();
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrieveRequestDump> > cta::
  Scheduler::getRetrieveRequests(const SecurityIdentity &requester) const {
  return m_db.getRetrieveRequests();
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveRequestDump> cta::Scheduler::getRetrieveRequests(
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
  CreationLog log(requester.getUser(), requester.getHost(), time(NULL),
     comment);
  m_db.createAdminHost(hostName, log);
}

//------------------------------------------------------------------------------
// createAdminHostWithoutAuthorizingRequester
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHostWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  CreationLog log (requester.getUser(), requester.getHost(), time(NULL), comment);
  m_db.createAdminHost(hostName, log);
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
  if(0 == nbCopies) {
    std::ostringstream msg;
    msg << "Failed to create storage class " << name << " nbCopies must be"
      " greater than 0";
    throw exception::Exception(msg.str());
  }

  m_db.createStorageClass(name, nbCopies, CreationLog(requester.getUser(), 
       requester.getHost(), time(NULL), comment));
  m_ns.createStorageClass(requester, name, nbCopies);
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const uint32_t id,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  if(0 == nbCopies) {
    std::ostringstream msg;
    msg << "Failed to create storage class " << name << " nbCopies must be"
      " greater than 0";
    throw exception::Exception(msg.str());
  }
  if(9999 < id) {
    std::ostringstream msg;
    msg << "Failed to create storage class " << name << " with numeric"
    " identifier " << id << " because the identifier is greater than the"
    " maximum permitted value of 9999";
    throw exception::Exception(msg.str());
  }
  m_db.createStorageClass(name, nbCopies, CreationLog(requester.getUser(), 
       requester.getHost(), time(NULL), comment));
  m_ns.createStorageClass(requester, name, nbCopies, id);
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
  m_ns.deleteStorageClass(requester, name);
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
  m_db.createTapePool(name, nbPartialTapes, CreationLog(requester.getUser(), 
    requester.getHost(), time(NULL), comment));
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
// getTapePools
//------------------------------------------------------------------------------
void cta::Scheduler::setTapePoolMountCriteria(
  const std::string & tapePool, const MountCriteriaByDirection & mountCriteriaByDirection) {
  return m_db.setTapePoolMountCriteria(tapePool, mountCriteriaByDirection);
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::createArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createArchiveRoute(storageClassName, copyNb, tapePoolName,
    CreationLog(UserIdentity(requester.getUser()), requester.getHost(),
    time(NULL), comment));
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.deleteArchiveRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute> cta::Scheduler::getArchiveRoutes(
  const SecurityIdentity &requester) const {
  return m_db.getArchiveRoutes();
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  m_db.createLogicalLibrary(name, CreationLog(requester.getUser(),
      requester.getHost(), time(NULL), comment));
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
  const std::string &density,
  const std::string & comment) {
  m_db.assertIsAdminOnAdminHost(requester);
  cta::CreationLog cl (requester.getUser(), requester.getHost(), time(NULL), comment);
  m_db.createTape(vid, logicalLibraryName, tapePoolName,
    capacityInBytes, density, cl);
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
// getTape
//------------------------------------------------------------------------------
cta::Tape cta::Scheduler::getTape(const SecurityIdentity &requester,
  const std::string &vid) const {
  return m_db.getTape(vid);
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
  const mode_t mode) {
  m_ns.createDir(requester, path, mode);
}

//------------------------------------------------------------------------------
// setOwner
//------------------------------------------------------------------------------
void cta::Scheduler::setOwner(
  const SecurityIdentity &requester,
  const std::string &path,
  const UserIdentity &owner) {
  m_ns.setOwner(requester, path, owner);
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
cta::UserIdentity cta::Scheduler::getOwner(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.getOwner(requester, path);
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
cta::ArchiveDirIterator cta::Scheduler::getDirContents(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.getDirContents(requester, path);
}

//------------------------------------------------------------------------------
// statArchiveFile
//------------------------------------------------------------------------------
std::unique_ptr<cta::ArchiveFileStatus> cta::Scheduler::statArchiveFile(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return m_ns.statFile(requester, path);
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

  const std::unique_ptr<ArchiveFileStatus> archiveStat =
    m_ns.statFile(requester, archiveFileOrDir);
  const bool archiveToDir = archiveStat.get() && S_ISDIR(archiveStat->mode);
  if(archiveToDir) {
    const std::string &archiveDir = archiveFileOrDir;
    if(remoteFiles.empty()) {
      std::ostringstream message;
      message << "Invalid archive to directory request:"
        " The must be at least one remote file:"
        " archiveDir=" << archiveDir;
      throw exception::Exception(message.str());
    }

    // Fetch the status of each remote file
    std::list<RemotePathAndStatus> rfil;
    for (auto rf = remoteFiles.begin(); rf != remoteFiles.end(); rf++) {
      const auto rfStat = m_remoteNS.statFile(*rf);
      if(NULL == rfStat.get()) {
        std::ostringstream msg;
        msg << "Failed to fetch status of remote file " << *rf <<
          " because the file does not exist";
        throw exception::Exception(msg.str());
      }
      rfil.push_back(RemotePathAndStatus(*rf, *rfStat));
    }
//    queueArchiveToDirRequest(requester, rfil, archiveDir);
    throw exception::Exception("Archive to Dir not supported in current prototype");

  // Else archive to a single file
  } else {
    const std::string &archiveFile = archiveFileOrDir;
    if(archiveStat.get()) {
      std::ostringstream msg;
      msg << "Failed to archive to a single file because the destination file "
        << archiveFile << " already exists in the archive namespace";
    }

    if(1 != remoteFiles.size()) {
      std::ostringstream message;
      message << "Invalid archive to file request:"
        " The must be one and only one remote file:"
        " actual=" << remoteFiles.size() << " archiveFile=" << archiveFile;
      throw exception::Exception(message.str());
    }
    const auto rfStat = m_remoteNS.statFile(remoteFiles.front());
    if(NULL == rfStat.get()) {
      std::ostringstream msg;
      msg << "Failed to fetch status of remote file " << remoteFiles.front() <<
        " because the file does not exist";
      throw exception::Exception(msg.str());
    }
    queueArchiveToFileRequest(requester, 
      RemotePathAndStatus(remoteFiles.front(), *rfStat), archiveFile);
  }
}

//------------------------------------------------------------------------------
// queueArchiveToDirRequest
//------------------------------------------------------------------------------
//void cta::Scheduler::queueArchiveToDirRequest(
//  const SecurityIdentity &requester,
//  const std::list<RemotePathAndStatus> &remoteFiles,
//  const std::string &archiveDir) {
//
//  const uint64_t priority = 0; // TO BE DONE
//
//  const auto storageClassName = m_ns.getDirStorageClass(requester, archiveDir);
//  const auto storageClass = m_db.getStorageClass(storageClassName);
//  assertStorageClassHasAtLeastOneCopy(storageClass);
//  const auto archiveToFileRequests = createArchiveToFileRequests(requester,
//    remoteFiles, archiveDir, priority);
//
//  CreationLog log(requester.getUser(), requester.getHost(), time(NULL), "");
//  m_db.queue(ArchiveToDirRequest(archiveDir, archiveToFileRequests, priority,
//    log));
//
//  for(auto itor = archiveToFileRequests.cbegin(); itor !=
//    archiveToFileRequests.cend(); itor++) {
//    createNSEntryAndUpdateSchedulerDatabase(requester, *itor);
//  }
//}

//------------------------------------------------------------------------------
// assertStorageClassHasAtLeastOneCopy
//------------------------------------------------------------------------------
void cta::Scheduler::assertStorageClassHasAtLeastOneCopy(
  const StorageClass &storageClass) const {
  if(0 == storageClass.nbCopies) {
    std::ostringstream message;
    message << "Storage class " << storageClass.name <<
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
  const std::list<RemotePathAndStatus> &remoteFiles,
  const std::string &archiveDir,
  const uint64_t priority) {
  const bool archiveDirEndsWithASlash = Utils::endsWith(archiveDir, '/');
  std::list<ArchiveToFileRequest> archiveToFileRequests;

  for(auto itor = remoteFiles.cbegin(); itor != remoteFiles.cend(); itor++) {
    const auto remoteFile = *itor;
    const auto remoteFileName = m_remoteNS.getFilename(remoteFile.path);
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
  const RemotePathAndStatus &remoteFile,
  const std::string &archiveFile) {

  const uint64_t priority = 0; // TO BE DONE
  m_ns.createFile(requester, archiveFile, remoteFile.status.mode, remoteFile.status.size);
  const ArchiveToFileRequest rqst = createArchiveToFileRequest(requester, remoteFile, archiveFile, priority);
  std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCreation> requestCreation(m_db.queue(rqst));
  requestCreation->complete();
}

//------------------------------------------------------------------------------
// queueArchiveToFileRequest
//------------------------------------------------------------------------------
//void cta::Scheduler::queueArchiveToFileRequest(
//  const SecurityIdentity &requester,
//  const RemotePathAndStatus &remoteFile,
//  const std::string &archiveFile) {
//
//  const uint64_t priority = 0; // TO BE DONE
//  const ArchiveToFileRequest rqst = createArchiveToFileRequest(requester,
//    remoteFile, archiveFile, priority);
//
//  std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCreation> 
//    requestCreation (m_db.queue(rqst));
//  try {
//    m_ns.createFile(requester, rqst.archiveFile.path, remoteFile.status.mode, remoteFile.status.size);
//  } catch(std::exception &nsEx) {
//    // Try our best to cleanup the scheduler database.  The garbage collection
//    // logic will try again if we fail.
//    try {
//      requestCreation->cancel();
//      //m_db.deleteArchiveRequest(requester, rqst.archiveFile);
//    } catch(...) {
//    }
//
//    // Whether or not we were able to clean up the scheduler database, the real
//    // problem here was the failure to create an entry in the NS
//    throw nsEx;
//  }
//  requestCreation->complete();
//}

//------------------------------------------------------------------------------
// createArchiveToFileRequest
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest cta::Scheduler::createArchiveToFileRequest(
  const SecurityIdentity &requester,
  const RemotePathAndStatus &remoteFile,
  const std::string &archiveFilePath,
  const uint64_t priority) const {

  const std::string enclosingPath = Utils::getEnclosingPath(archiveFilePath);
  const std::string storageClassName = m_ns.getDirStorageClass(requester,
     enclosingPath);
  const StorageClass storageClass = m_db.getStorageClass(storageClassName);
  assertStorageClassHasAtLeastOneCopy(storageClass);
  const auto routes = m_db.getArchiveRoutes(storageClassName);
  const auto copyNbToPoolMap = createCopyNbToPoolMap(routes);
  std::unique_ptr<cta::ArchiveFileStatus> status = m_ns.statFile(requester, archiveFilePath);
  cta::ArchiveFile archiveFile(archiveFilePath, "", status->fileId, remoteFile.status.size, 0, 0);

  const CreationLog log(requester.getUser(), requester.getHost(), time(NULL));
  return ArchiveToFileRequest(
    remoteFile,
    archiveFile,
    copyNbToPoolMap,
    priority,
    log);
}

//------------------------------------------------------------------------------
// createCopyNbToPoolMap
//------------------------------------------------------------------------------
std::map<uint16_t, std::string> cta::Scheduler::createCopyNbToPoolMap(
  const std::list<ArchiveRoute> &routes) const {
  std::map<uint16_t, std::string> copyNbToPoolMap;
    for(auto itor = routes.begin(); itor != routes.end(); itor++) {
    const ArchiveRoute &route = *itor;
    copyNbToPoolMap[route.copyNb] = route.tapePoolName;
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

  const auto remoteStat = m_remoteNS.statFile(remoteFileOrDir);
  // If the target does not exist or is a directory, we are retrieving to 
  // directory
  const bool retrieveToDir = remoteStat.get() && S_ISDIR(remoteStat->mode);
  if(retrieveToDir) {
    const std::string &remoteDir = remoteFileOrDir;
    std::ostringstream msg;
    msg << "Failed to queue request to retrieve to the " << remoteDir <<
      " directory because this functionality is not impelemented";
    throw exception::Exception(msg.str());
  // Else retrieve to a single file
  } else {
    const std::string &remoteFile = remoteFileOrDir;

    // The remote file should not exist
    if(remoteStat.get()) {
      std::ostringstream msg;
      msg << "Failed to queue request to retrieve to the single file " <<
        remoteFile << " because the remote file already exists";
      throw exception::Exception(msg.str());
    }
    // We should only try to retrieve one file
    if (archiveFiles.size() != 1)
      throw exception::Exception("Failed to queue request to retrieve to single file: trying to retrieve several files");
    // Check the validity of the source
    auto sourceStat = m_ns.statFile(requester, archiveFiles.front());
    if (!sourceStat.get())
      throw exception::Exception("Failed to queue request to retrieve to single file: source file does not exist");
    auto tapeCopies = m_ns.getTapeFiles(requester, archiveFiles.front());
    if (!tapeCopies.size())
      throw exception::Exception("Failed to queue request to retrieve to single file: source file has no copy on tape");
    // Generate the requests and enqueue them in the database
    CreationLog cl (requester.getUser(), requester.getHost(), time(NULL), 
        "Retrieve request queueing");
    std::list<TapeFileLocation> tcl;
    for (auto nstf = tapeCopies.begin(); nstf != tapeCopies.end(); nstf++) {
      tcl.push_back(nstf->tapeFileLocation);
    }
    cta::ArchiveFile archiveFile(archiveFiles.front(), "", sourceStat->fileId, sourceStat->size, *((uint32_t*)(sourceStat->checksum.getByteArray().getBytes())), 0); //nshostname is set to "" and lastmodificationtime is 0 because we don't need this info for retrieving the file
    RetrieveToFileRequest rtfr (archiveFile,
        tcl, remoteFile, 0, cl);
    m_db.queue(rtfr);
  }
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::Scheduler::getNextMount(
  const std::string &logicalLibraryName, const std::string & driveName) {
  // In order to decide the next mount to do, we have to take a global lock on 
  // the scheduling, retrieve a list of all running mounts, queues sizes for 
  // tapes and tape pools, filter the tapes which are actually accessible to
  // this drive (by library and dedication), order the candidates by priority
  // below threshold, and pick one at a time. In addition, for archives, we
  // might not find a suitable tape (by library and dedication). In such a case,
  // we should find out if no tape at all is available, and log an error if 
  // so.
  // We then skip to the next candidate, until we find a suitable one and
  // return the mount, or exhaust all of them an 
  // Many steps for this logic are not specific for the database and are hence
  // implemented in the scheduler itself.
  // First, get the mount-related info from the DB
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfo();
  auto potentialMounts = mountInfo->potentialMounts;
  
  // We should now filter the potential mounts to keep only the ones we are
  // compatible with (match the logical library for retrieves).
  // We also only want the potential mounts for which we still have 
  // We cannot filter the archives yet
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    if (m->type == MountType::RETRIEVE && m->logicalLibrary != logicalLibraryName) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      m++;
    }
  }
  
  // With the existing mount list, we can now populate the potential mount list
  // with the per tape pool existing mount statistics.
  typedef std::pair<std::string, cta::MountType::Enum> tpType;
  std::map<tpType, uint32_t> existingMountsSummary;
  for (auto em=mountInfo->existingMounts.begin(); em!=mountInfo->existingMounts.end(); em++) {
    try {
      existingMountsSummary.at(tpType(em->tapePool, em->type))++;
    } catch (std::out_of_range &) {
      existingMountsSummary[tpType(em->tapePool, em->type)] = 1;
    }
  }
  
  // We can now filter out the potential mounts for which their mount criteria
  // is already met, filter out the potential mounts for which the maximum mount
  // quota is already reached, and weight the remaining by how much of their quota 
  // is reached
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    // Get summary data
    uint32_t existingMounts;
    try {
      existingMounts = existingMountsSummary.at(tpType(m->tapePool, m->type));
    } catch (std::out_of_range &) {
      existingMounts = 0;
    } 
    bool mountPassesACriteria = false;
    if (m->bytesQueued / (1 + existingMounts) >= m->mountCriteria.maxBytesQueued)
      mountPassesACriteria = true;
    if (m->filesQueued / (1 + existingMounts) >= m->mountCriteria.maxFilesQueued)
      mountPassesACriteria = true;
    if (!existingMounts && ((time(NULL) - m->oldestJobStartTime) > (int64_t)m->mountCriteria.maxAge))
      mountPassesACriteria = true;
    if (!mountPassesACriteria || existingMounts >= m->mountCriteria.quota) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      // populate the mount with a weight 
      m->ratioOfMountQuotaUsed = 1.0L * existingMounts / m->mountCriteria.quota;
      m++;
   }
  }
  
  // We can now sort the potential mounts in decreasing priority order. 
  // The ordering is defined in operator <.
  // We want the result in descending order or priority so we reverse the vector
  std::sort(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  std::reverse(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  
  // We can now simply iterate on the candidates until we manage to create a
  // mount for one of them
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (m->type==cta::MountType::ARCHIVE) {
      // We need to find a tape for archiving. It should be both in the right 
      // tape pool and in the drive's logical library
      auto tapesList = m_db.getTapes();
      // The first tape matching will go for a prototype.
      // TODO: improve to reuse already partially written tapes
      for (auto t=tapesList.begin(); t!=tapesList.end(); t++) {
        if (t->logicalLibraryName == logicalLibraryName &&
            t->tapePoolName == m->tapePool &&
            t->status.availableToWrite()) {
          // We have our tape. Try to create the session. Prepare a return value
          // for it.
          std::unique_ptr<ArchiveMount> internalRet(new ArchiveMount(m_ns));
          // Get the db side of the session
          try {
            internalRet->m_dbMount.reset(mountInfo->createArchiveMount(t->vid,
                t->tapePoolName, 
                driveName, 
                logicalLibraryName, 
                Utils::getShortHostname(), 
                time(NULL)).release());
            internalRet->m_sessionRunning = true;
            return std::unique_ptr<TapeMount> (internalRet.release());
          } catch (cta::exception::Exception & ex) {
            continue;
          }
        }
      }
    } else if (m->type==cta::MountType::RETRIEVE) {
      // We know the tape we intend to mount. We have to validate the tape is 
      // actually available to read, and pass on it if no.
      auto tapesList = m_db.getTapes();
      for (auto t=tapesList.begin(); t!=tapesList.end(); t++) {
        if (t->vid == m->vid && t->status.availableToRead()) {
          try {
            // create the mount, and populate its DB side.
            std::unique_ptr<RetrieveMount> internalRet (
              new RetrieveMount(mountInfo->createRetrieveMount(t->vid, 
                t->tapePoolName,
                driveName,
                logicalLibraryName, 
                Utils::getShortHostname(), 
                time(NULL))));
            internalRet->m_sessionRunning = true;
            return std::unique_ptr<TapeMount> (internalRet.release()); 
         } catch (cta::exception::Exception & ex) {
           std::string debug=ex.getMessageValue();
           continue;
         }
        }
      }
    } else {
      throw std::runtime_error("In Scheduler::getNextMount unexpected mount type");
    }
  }
  return std::unique_ptr<TapeMount>();
}

//
////------------------------------------------------------------------------------
//// finishedMount
////------------------------------------------------------------------------------
//void cta::Scheduler::finishedMount(const std::string &mountId) {
//}
//
////------------------------------------------------------------------------------
//// getNextArchive
////------------------------------------------------------------------------------
//cta::ArchiveJob *cta::Scheduler::getNextArchive(
//  const std::string &mountId) {
//  return NULL;
//}
//
////------------------------------------------------------------------------------
//// archiveSuccessful
////------------------------------------------------------------------------------
//void cta::Scheduler::archiveSuccessful(const std::string &transferId) {
//}
//
////------------------------------------------------------------------------------
//// archiveFailed
////------------------------------------------------------------------------------
//void cta::Scheduler::archiveFailed(const std::string &transferId,
//  const TapeJobFailure failureType, const std::string &errorMessage) {
//}
//
////------------------------------------------------------------------------------
//// getNextRetrieve
////------------------------------------------------------------------------------
//cta::RetrieveJob *cta::Scheduler::getNextRetrieve(
//  const std::string &mountId) {
//  return NULL;
//}
//
////------------------------------------------------------------------------------
//// retrieveSucceeded
////------------------------------------------------------------------------------
//void cta::Scheduler::retrieveSucceeded(const std::string &transferId) {
//}
//
////------------------------------------------------------------------------------
//// retrieveFailed
////------------------------------------------------------------------------------
//void cta::Scheduler::retrieveFailed(const std::string &transferId,
//  const TapeJobFailure failureType, const std::string &errorMessage) {
//}
