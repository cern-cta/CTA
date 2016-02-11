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
#include "common/utils/Utils.hpp"
#include "common/SecurityIdentity.hpp"
#include "common/TapePool.hpp"
#include "nameserver/NameServer.hpp"
#include "remotens/RemoteNS.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/DummyScheduler.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DummyScheduler::DummyScheduler(NameServer &ns,
  SchedulerDatabase &db,
  RemoteNS &remoteNS): Scheduler(ns, db, remoteNS) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::DummyScheduler::~DummyScheduler() throw() {
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchiveToTapeCopyRequest> >
  cta::DummyScheduler::getArchiveRequests(const SecurityIdentity &requester) const {
  return std::map<cta::TapePool, std::list<cta::ArchiveToTapeCopyRequest> >();
}

//------------------------------------------------------------------------------
// getArchiveRequests
//------------------------------------------------------------------------------
std::list<cta::ArchiveToTapeCopyRequest> cta::DummyScheduler::getArchiveRequests(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  return std::list<cta::ArchiveToTapeCopyRequest>();
}

//------------------------------------------------------------------------------
// deleteArchiveRequest
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteArchiveRequest(
  const SecurityIdentity &requester,
  const std::string &archiveFile) {
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrieveRequestDump> > cta::
  DummyScheduler::getRetrieveRequests(const SecurityIdentity &requester) const {
  return std::map<cta::Tape, std::list<cta::RetrieveRequestDump> >();
}

//------------------------------------------------------------------------------
// getRetrieveRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveRequestDump> cta::DummyScheduler::getRetrieveRequests(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  return std::list<cta::RetrieveRequestDump>();
}
  
//------------------------------------------------------------------------------
// deleteRetrieveRequest
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteRetrieveRequest(
  const SecurityIdentity &requester,
  const std::string &remoteFile) {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::DummyScheduler::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createAdminUserWithoutAuthorizingRequester
//------------------------------------------------------------------------------
void cta::DummyScheduler::createAdminUserWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::admin::AdminUser> cta::DummyScheduler::getAdminUsers(const SecurityIdentity
  &requester) const {
  return std::list<common::admin::AdminUser>();
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::DummyScheduler::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createAdminHostWithoutAuthorizingRequester
//------------------------------------------------------------------------------
void cta::DummyScheduler::createAdminHostWithoutAuthorizingRequester(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::admin::AdminHost> cta::DummyScheduler::getAdminHosts(const SecurityIdentity
  &requester) const {
  return std::list<cta::common::admin::AdminHost>();
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::DummyScheduler::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::DummyScheduler::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const uint32_t id,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::DummyScheduler::getStorageClasses(
  const SecurityIdentity &requester) const {
  return std::list<cta::StorageClass>();
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::DummyScheduler::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::DummyScheduler::getTapePools(
  const SecurityIdentity &requester) const {
  return std::list<cta::TapePool>();
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::DummyScheduler::createArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::archiveRoute::ArchiveRoute> cta::DummyScheduler::getArchiveRoutes(
  const SecurityIdentity &requester) const {
  return std::list<cta::common::archiveRoute::ArchiveRoute>();
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::DummyScheduler::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::DummyScheduler::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return std::list<cta::LogicalLibrary>();
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::DummyScheduler::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const CreationLog &creationLog) {
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// getTape
//------------------------------------------------------------------------------
cta::Tape cta::DummyScheduler::getTape(const SecurityIdentity &requester,
  const std::string &vid) const {
  return Tape();
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::DummyScheduler::getTapes(
  const SecurityIdentity &requester) const {
  return std::list<cta::Tape>();
}

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::DummyScheduler::createDir(
  const SecurityIdentity &requester,
  const std::string &path,
  const mode_t mode) {
}

//------------------------------------------------------------------------------
// setOwner
//------------------------------------------------------------------------------
void cta::DummyScheduler::setOwner(
  const SecurityIdentity &requester,
  const std::string &path,
  const UserIdentity &owner) {
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
cta::UserIdentity cta::DummyScheduler::getOwner(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return cta::UserIdentity();
}

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::DummyScheduler::deleteDir(
  const SecurityIdentity &requester,
  const std::string &path) {
}

//------------------------------------------------------------------------------
// getVidOfFile
//------------------------------------------------------------------------------
std::string cta::DummyScheduler::getVidOfFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t copyNb) const {
  return std::string();
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::common::archiveNS::ArchiveDirIterator cta::DummyScheduler::getDirContents(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return cta::common::archiveNS::ArchiveDirIterator();
}

//------------------------------------------------------------------------------
// statArchiveFile
//------------------------------------------------------------------------------
std::unique_ptr<cta::common::archiveNS::ArchiveFileStatus> cta::DummyScheduler::statArchiveFile(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return std::unique_ptr<cta::common::archiveNS::ArchiveFileStatus>();
}

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::DummyScheduler::setDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path,
  const std::string &storageClassName) {
}

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::DummyScheduler::clearDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) {
}
  
//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::DummyScheduler::getDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) const {
  return std::string();
}

//------------------------------------------------------------------------------
// queueArchiveRequest
//------------------------------------------------------------------------------
void cta::DummyScheduler::queueArchiveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &remoteFiles,
  const std::string &archiveFileOrDir) {
}

//------------------------------------------------------------------------------
// queueRetrieveRequest
//------------------------------------------------------------------------------
void cta::DummyScheduler::queueRetrieveRequest(
  const SecurityIdentity &requester,
  const std::list<std::string> &archiveFiles,
  const std::string &remoteFileOrDir) {
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::DummyScheduler::getNextMount(
  const std::string &logicalLibraryName, const std::string & driveName) {
  return std::unique_ptr<cta::TapeMount>();
}
