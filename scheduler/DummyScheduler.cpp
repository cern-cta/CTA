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

#include "scheduler/DummyScheduler.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
Scheduler() {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
~Scheduler() throw() {
}

//-----------------------------------------------------------------------------
// getArchiveRequests
//-----------------------------------------------------------------------------
std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > getArchiveRequests(
  const SecurityIdentity &requester) const;

//-----------------------------------------------------------------------------
// getArchiveRequests
//-----------------------------------------------------------------------------
std::list<ArchiveToTapeCopyRequest> getArchiveRequests(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const;

//-----------------------------------------------------------------------------
// deleteArchiveRequest
//-----------------------------------------------------------------------------
void deleteArchiveRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile);

//-----------------------------------------------------------------------------
// getRetrieveRequests
//-----------------------------------------------------------------------------
std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > getRetrieveRequests(
  const SecurityIdentity &requester) const;

// getRetrieveRequests
std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(
  const SecurityIdentity &requester,
  const std::string &vid) const;

void deleteRetrieveRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile);

void createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment);

void createAdminUserWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment);

void deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user);

std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester) const;

void createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment);

void createAdminHostWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment);

void deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName);

std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const;

void createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment);

void createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const uint32_t id,
  const std::string &comment);

void deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name);

std::list<StorageClass> getStorageClasses(
  const SecurityIdentity &requester) const;

void createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment);

void deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name);

std::list<TapePool> getTapePools(
  const SecurityIdentity &requester) const;

void createArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment);

void deleteArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb);

std::list<ArchiveRoute> getArchiveRoutes(
  const SecurityIdentity &requester) const;

void createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment);

void deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name);

std::list<LogicalLibrary> getLogicalLibraries(
  const SecurityIdentity &requester) const;

void createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const CreationLog &creationLog);

void deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid);

Tape getTape(
  const SecurityIdentity &requester,
  const std::string &vid) const;

std::list<Tape> getTapes(
  const SecurityIdentity &requester) const;

void createDir(
  const SecurityIdentity &requester,
  const std::string &path,
  const mode_t mode);

void setOwner(
  const SecurityIdentity &requester,
  const std::string &path,
  const UserIdentity &owner);
    
UserIdentity getOwner(
  const SecurityIdentity &requester,
  const std::string &path) const;

void deleteDir(
 const SecurityIdentity &requester,
 const std::string &path);

std::string getVidOfFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t copyNb) const;

ArchiveDirIterator getDirContents(
  const SecurityIdentity &requester,
  const std::string &path) const;

std::unique_ptr<ArchiveFileStatus> statArchiveFile(
  const SecurityIdentity &requester,
  const std::string &path) const;

void setDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path,
  const std::string &storageClassName);

void clearDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path);

std::string getDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) const;

void queueArchiveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &remoteFiles,
  const std::string &archiveFileOrDir);

void queueRetrieveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &archiveFiles,
  const std::string &remoteFileOrDir);

std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName,
  const std::string & driveName);
