/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY {

} without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "scheduler/Scheduler.hpp"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>

//------------------------------------------------------------------------------
// deprecated constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler(
  catalogue::Catalogue &catalogue,
  NameServer &ns,
  SchedulerDatabase &db,
  RemoteNS &remoteNS): m_catalogue(catalogue), m_db(db) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Scheduler::~Scheduler() throw() {
}

//------------------------------------------------------------------------------
// queueArchiveRequest
//------------------------------------------------------------------------------
uint64_t cta::Scheduler::queueArchiveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ArchiveRequest &request) {  
  const uint64_t archiveFileId = m_catalogue.getNextArchiveFileId();
  std::unique_ptr<SchedulerDatabase::ArchiveToFileRequestCreation> requestCreation(m_db.queue(request, archiveFileId));
  requestCreation->complete();
  
  return 0;
}

//------------------------------------------------------------------------------
// queueRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::RetrieveRequest &request) {

}

//------------------------------------------------------------------------------
// deleteArchiveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::DeleteArchiveRequest &request) {

}

//------------------------------------------------------------------------------
// cancelRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::CancelRetrieveRequest &request) {

}

//------------------------------------------------------------------------------
// updateFileInfoRequest
//------------------------------------------------------------------------------
void cta::Scheduler::updateFileInfoRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::UpdateFileInfoRequest &request) {

}

//------------------------------------------------------------------------------
// listStorageClassRequest
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::listStorageClassRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ListStorageClassRequest &request) {
  return std::list<cta::common::dataStructures::StorageClass>();
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::Scheduler::createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, 
        const std::string &comment) {
  m_catalogue.createBootstrapAdminAndHostNoAuth(requester, user, hostName, comment);
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {
  m_catalogue.createAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user) {
  m_catalogue.deleteAdminUser(requester, user);
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser> cta::Scheduler::getAdminUsers(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getAdminUsers(requester);
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {
  m_catalogue.modifyAdminUserComment(requester, user, comment);
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {
  m_catalogue.createAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName) {
  m_catalogue.deleteAdminHost(requester, hostName);
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::Scheduler::getAdminHosts(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getAdminHosts(requester); 
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {
  m_catalogue.modifyAdminHostComment(requester, hostName, comment);
}


//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::createStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies, const std::string &comment) {
  m_catalogue.createStorageClass(requester, name, nbCopies, comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {
  m_catalogue.deleteStorageClass(requester, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::getStorageClasses(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getStorageClasses(requester); 
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies) {
  m_catalogue.modifyStorageClassNbCopies(requester, name, nbCopies);
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  m_catalogue.modifyStorageClassComment(requester, name, comment);
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::createTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes, const std::string &comment) {
  m_catalogue.createTapePool(requester, name, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {
  m_catalogue.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::Scheduler::getTapePools(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getTapePools(requester); 
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes) {
  m_catalogue.modifyTapePoolNbPartialTapes(requester, name, nbPartialTapes);
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  m_catalogue.modifyTapePoolComment(requester, name, comment);
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName,
        const std::string &comment) {
  m_catalogue.createArchiveRoute(requester, storageClassName, copyNb, tapePoolName, comment);
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb) {
  m_catalogue.deleteArchiveRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::Scheduler::getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getArchiveRoutes(requester); 
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {
  m_catalogue.modifyArchiveRouteTapePoolName(requester, storageClassName, copyNb, tapePoolName);
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {
  m_catalogue.modifyArchiveRouteComment(requester, storageClassName, copyNb, comment);
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  m_catalogue.createLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {
  m_catalogue.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::Scheduler::getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getLogicalLibraries(requester); 
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  m_catalogue.modifyLogicalLibraryComment(requester, name, comment);
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::Scheduler::createTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) {
  m_catalogue.createTape(requester, vid, logicalLibraryName, tapePoolName, capacityInBytes, disabledValue, fullValue, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {
  m_catalogue.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::Scheduler::getTapes(const cta::common::dataStructures::SecurityIdentity &requester,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue) {
  return m_catalogue.getTapes(requester, vid, logicalLibraryName, tapePoolName, capacityInBytes, disabledValue, fullValue, busyValue); 
}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void cta::Scheduler::labelTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::Scheduler::reclaimTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {
  m_catalogue.reclaimTape(requester, vid);
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) {
  m_catalogue.modifyTapeLogicalLibraryName(requester, vid, logicalLibraryName);
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) {
  m_catalogue.modifyTapeTapePoolName(requester, vid, tapePoolName);
}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) {
  m_catalogue.modifyTapeCapacityInBytes(requester, vid, capacityInBytes);
}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeBusy(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue) {
  m_catalogue.setTapeBusy(requester, vid, busyValue);
}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeFull(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue) {
  m_catalogue.setTapeFull(requester, vid, fullValue);
}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue) {
  m_catalogue.setTapeDisabled(requester, vid, disabledValue);
}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment) {
  m_catalogue.modifyTapeComment(requester, vid, comment);
}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::Scheduler::createUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup,
        const std::string &comment) {
  m_catalogue.createUser(requester, name, group, userGroup, comment);
}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group) {
  m_catalogue.deleteUser(requester, name, group);
}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::Scheduler::getUsers(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getUsers(requester); 
}

//------------------------------------------------------------------------------
// modifyUserUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) {
  m_catalogue.modifyUserUserGroup(requester, name, group, userGroup);
}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) {
  m_catalogue.modifyUserComment(requester, name, group, comment);
}

//------------------------------------------------------------------------------
// createUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::createUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued,
        const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
        const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {
  m_catalogue.createUserGroup(requester, name, archivePriority, minArchiveFilesQueued, minArchiveBytesQueued, minArchiveRequestAge, retrievePriority, minRetrieveFilesQueued, minRetrieveBytesQueued, minRetrieveRequestAge, maxDrivesAllowed, comment);
}

//------------------------------------------------------------------------------
// deleteUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::deleteUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {
  m_catalogue.deleteUserGroup(requester, name);
}

//------------------------------------------------------------------------------
// getUserGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::UserGroup> cta::Scheduler::getUserGroups(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getUserGroups(requester); 
}

//------------------------------------------------------------------------------
// modifyUserGroupArchivePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority) {
  m_catalogue.modifyUserGroupArchivePriority(requester, name, archivePriority);
}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) {
  m_catalogue.modifyUserGroupArchiveMinFilesQueued(requester, name, minArchiveFilesQueued);
}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) {
  m_catalogue.modifyUserGroupArchiveMinBytesQueued(requester, name, minArchiveBytesQueued);
}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) {
  m_catalogue.modifyUserGroupArchiveMinRequestAge(requester, name, minArchiveRequestAge);
}

//------------------------------------------------------------------------------
// modifyUserGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t retrievePriority) {
  m_catalogue.modifyUserGroupRetrievePriority(requester, name, retrievePriority);
}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) {
  m_catalogue.modifyUserGroupRetrieveMinFilesQueued(requester, name, minRetrieveFilesQueued);
}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) {
  m_catalogue.modifyUserGroupRetrieveMinBytesQueued(requester, name, minRetrieveBytesQueued);
}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) {
  m_catalogue.modifyUserGroupRetrieveMinRequestAge(requester, name, minRetrieveRequestAge);
}

//------------------------------------------------------------------------------
// modifyUserGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t maxDrivesAllowed) {
  m_catalogue.modifyUserGroupMaxDrivesAllowed(requester, name, maxDrivesAllowed);
}

//------------------------------------------------------------------------------
// modifyUserGroupComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {
  m_catalogue.modifyUserGroupComment(requester, name, comment);
}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::Scheduler::createDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &userGroup,
        const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {
  m_catalogue.createDedication(requester, drivename, dedicationType, userGroup, tag, vid, fromTimestamp, untilTimestamp, comment);
}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::Scheduler::deleteDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename) {
  m_catalogue.deleteDedication(requester, drivename);
}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::Scheduler::getDedications(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return m_catalogue.getDedications(requester); 
}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {
  m_catalogue.modifyDedicationType(requester, drivename, dedicationType);
}

//------------------------------------------------------------------------------
// modifyDedicationUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) {
  m_catalogue.modifyDedicationUserGroup(requester, drivename, userGroup);
}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag) {
  m_catalogue.modifyDedicationTag(requester, drivename, tag);
}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid) {
  m_catalogue.modifyDedicationVid(requester, drivename, vid);
}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) {
  m_catalogue.modifyDedicationFrom(requester, drivename, fromTimestamp);
}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) {
  m_catalogue.modifyDedicationUntil(requester, drivename, untilTimestamp);
}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment) {
  m_catalogue.modifyDedicationComment(requester, drivename, comment);
}

//------------------------------------------------------------------------------
// repack
//------------------------------------------------------------------------------
void cta::Scheduler::repack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tag, const cta::common::dataStructures::RepackType) {
}

//------------------------------------------------------------------------------
// cancelRepack
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRepack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {
}

//------------------------------------------------------------------------------
// getRepacks
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RepackInfo> cta::Scheduler::getRepacks(const cta::common::dataStructures::SecurityIdentity &requester) {
  return std::list<cta::common::dataStructures::RepackInfo>(); 
}

//------------------------------------------------------------------------------
// getRepack
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo cta::Scheduler::getRepack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {
  return cta::common::dataStructures::RepackInfo(); 
}

//------------------------------------------------------------------------------
// shrink
//------------------------------------------------------------------------------
void cta::Scheduler::shrink(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &tapepool) {
}

//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
void cta::Scheduler::verify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tag, const uint64_t numberOfFiles) {
}

//------------------------------------------------------------------------------
// cancelVerify
//------------------------------------------------------------------------------
void cta::Scheduler::cancelVerify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {

}

//------------------------------------------------------------------------------
// getVerifys
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::VerifyInfo> cta::Scheduler::getVerifys(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::VerifyInfo>(); 
}

//------------------------------------------------------------------------------
// getVerify
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyInfo cta::Scheduler::getVerify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) const {
  return cta::common::dataStructures::VerifyInfo(); 
}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &requester, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return m_catalogue.getArchiveFiles(requester, id, eosid, copynb, tapepool, vid, owner, group, storageclass, path); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::Scheduler::getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &requester, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return m_catalogue.getArchiveFileSummary(requester, id, eosid, copynb, tapepool, vid, owner, group, storageclass, path);
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile cta::Scheduler::getArchiveFileById(const uint64_t id){
  return m_catalogue.getArchiveFileById(id);
}

//------------------------------------------------------------------------------
// readTest
//------------------------------------------------------------------------------
cta::common::dataStructures::ReadTestResult cta::Scheduler::readTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid,
        const uint64_t firstFSeq, const uint64_t lastFSeq, const bool checkChecksum, const std::string &output, const std::string &tag) const {
  return cta::common::dataStructures::ReadTestResult(); 
}

//------------------------------------------------------------------------------
// writeTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::writeTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid,
        const std::string &inputFile, const std::string &tag) const {
  return cta::common::dataStructures::WriteTestResult(); 
}

//------------------------------------------------------------------------------
// write_autoTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::write_autoTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid,
        const uint64_t numberOfFiles, const uint64_t fileSize, const cta::common::dataStructures::TestSourceType testSourceType, const std::string &tag) const {
  return cta::common::dataStructures::WriteTestResult(); 
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::Scheduler::setDriveStatus(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) {

}

//------------------------------------------------------------------------------
// reconcile
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::reconcile(const cta::common::dataStructures::SecurityIdentity &requester) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > cta::Scheduler::getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> >(); 
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob> cta::Scheduler::getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &tapePoolName) const {
  return std::list<cta::common::dataStructures::ArchiveJob>(); 
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > cta::Scheduler::getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> >(); 
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RetrieveJob> cta::Scheduler::getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) const {
  return std::list<cta::common::dataStructures::RetrieveJob>(); 
}

//------------------------------------------------------------------------------
// getDriveStates
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> cta::Scheduler::getDriveStates(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::DriveState>(); 
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::Scheduler::getNextMount(const std::string &logicalLibraryName, const std::string &driveName) {
  return std::unique_ptr<TapeMount>();
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::Scheduler::_old_getNextMount(const std::string &logicalLibraryName, const std::string & driveName) {
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
  return std::unique_ptr<TapeMount>();
}
