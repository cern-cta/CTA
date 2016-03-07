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
uint64_t cta::Scheduler::queueArchiveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ArchiveRequest &request) {  
  const uint64_t archiveFileId = m_catalogue.getNextArchiveFileId();
  const std::map<uint64_t, std::string> copyNbToPoolMap = m_catalogue.getCopyNbToTapePoolMap(request.storageClass);
  const cta::common::dataStructures::MountPolicy policy = m_catalogue.getArchiveMountPolicy(request.requester);
  std::unique_ptr<SchedulerDatabase::ArchiveRequestCreation> requestCreation(m_db.queue(request, archiveFileId, copyNbToPoolMap, policy));
  requestCreation->complete();
  
  return 0;
}

//------------------------------------------------------------------------------
// queueRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::RetrieveRequest &request) {

}

//------------------------------------------------------------------------------
// deleteArchiveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::DeleteArchiveRequest &request) {

}

//------------------------------------------------------------------------------
// cancelRetrieveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::CancelRetrieveRequest &request) {

}

//------------------------------------------------------------------------------
// updateFileInfoRequest
//------------------------------------------------------------------------------
void cta::Scheduler::updateFileInfoRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UpdateFileInfoRequest &request) {

}

//------------------------------------------------------------------------------
// listStorageClassRequest
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::listStorageClassRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ListStorageClassRequest &request) {
  return std::list<cta::common::dataStructures::StorageClass>();
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::Scheduler::createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, 
        const std::string &comment) {
  m_catalogue.createBootstrapAdminAndHostNoAuth(cliIdentity, user, hostName, comment);
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {
  m_catalogue.createAdminUser(cliIdentity, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user) {
  m_catalogue.deleteAdminUser(cliIdentity, user);
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser> cta::Scheduler::getAdminUsers(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getAdminUsers(cliIdentity);
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {
  m_catalogue.modifyAdminUserComment(cliIdentity, user, comment);
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHost(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {
  m_catalogue.createAdminHost(cliIdentity, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName) {
  m_catalogue.deleteAdminHost(cliIdentity, hostName);
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::Scheduler::getAdminHosts(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getAdminHosts(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {
  m_catalogue.modifyAdminHostComment(cliIdentity, hostName, comment);
}


//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::createStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment) {
  m_catalogue.createStorageClass(cliIdentity, name, nbCopies, comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {
  m_catalogue.deleteStorageClass(cliIdentity, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::getStorageClasses(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getStorageClasses(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) {
  m_catalogue.modifyStorageClassNbCopies(cliIdentity, name, nbCopies);
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  m_catalogue.modifyStorageClassComment(cliIdentity, name, comment);
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::createTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const std::string &comment) {
  m_catalogue.createTapePool(cliIdentity, name, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {
  m_catalogue.deleteTapePool(cliIdentity, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::Scheduler::getTapePools(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getTapePools(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) {
  m_catalogue.modifyTapePoolNbPartialTapes(cliIdentity, name, nbPartialTapes);
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  m_catalogue.modifyTapePoolComment(cliIdentity, name, comment);
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName,
        const std::string &comment) {
  m_catalogue.createArchiveRoute(cliIdentity, storageClassName, copyNb, tapePoolName, comment);
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb) {
  m_catalogue.deleteArchiveRoute(cliIdentity, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::Scheduler::getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getArchiveRoutes(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {
  m_catalogue.modifyArchiveRouteTapePoolName(cliIdentity, storageClassName, copyNb, tapePoolName);
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {
  m_catalogue.modifyArchiveRouteComment(cliIdentity, storageClassName, copyNb, comment);
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  m_catalogue.createLogicalLibrary(cliIdentity, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {
  m_catalogue.deleteLogicalLibrary(cliIdentity, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::Scheduler::getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getLogicalLibraries(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  m_catalogue.modifyLogicalLibraryComment(cliIdentity, name, comment);
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::Scheduler::createTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) {
  m_catalogue.createTape(cliIdentity, vid, logicalLibraryName, tapePoolName, capacityInBytes, disabledValue, fullValue, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  m_catalogue.deleteTape(cliIdentity, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::Scheduler::getTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue) {
  return m_catalogue.getTapes(cliIdentity, vid, logicalLibraryName, tapePoolName, capacityInBytes, disabledValue, fullValue, busyValue); 
}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void cta::Scheduler::labelTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::Scheduler::reclaimTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  m_catalogue.reclaimTape(cliIdentity, vid);
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) {
  m_catalogue.modifyTapeLogicalLibraryName(cliIdentity, vid, logicalLibraryName);
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) {
  m_catalogue.modifyTapeTapePoolName(cliIdentity, vid, tapePoolName);
}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) {
  m_catalogue.modifyTapeCapacityInBytes(cliIdentity, vid, capacityInBytes);
}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeBusy(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) {
  m_catalogue.setTapeBusy(cliIdentity, vid, busyValue);
}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeFull(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) {
  m_catalogue.setTapeFull(cliIdentity, vid, fullValue);
}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) {
  m_catalogue.setTapeDisabled(cliIdentity, vid, disabledValue);
}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) {
  m_catalogue.modifyTapeComment(cliIdentity, vid, comment);
}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::Scheduler::createUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup,
        const std::string &comment) {
  m_catalogue.createUser(cliIdentity, name, group, mountGroup, comment);
}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group) {
  m_catalogue.deleteUser(cliIdentity, name, group);
}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::Scheduler::getUsers(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getUsers(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyUserMountGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup) {
  m_catalogue.modifyUserMountGroup(cliIdentity, name, group, mountGroup);
}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &comment) {
  m_catalogue.modifyUserComment(cliIdentity, name, group, comment);
}

//------------------------------------------------------------------------------
// createMountGroup
//------------------------------------------------------------------------------
void cta::Scheduler::createMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued,
        const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
        const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {
  m_catalogue.createMountGroup(cliIdentity, name, archivePriority, minArchiveFilesQueued, minArchiveBytesQueued, minArchiveRequestAge, retrievePriority, minRetrieveFilesQueued, minRetrieveBytesQueued, minRetrieveRequestAge, maxDrivesAllowed, comment);
}

//------------------------------------------------------------------------------
// deleteMountGroup
//------------------------------------------------------------------------------
void cta::Scheduler::deleteMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name) {
  m_catalogue.deleteMountGroup(cliIdentity, name);
}

//------------------------------------------------------------------------------
// getMountGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::MountGroup> cta::Scheduler::getMountGroups(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getMountGroups(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyMountGroupArchivePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) {
  m_catalogue.modifyMountGroupArchivePriority(cliIdentity, name, archivePriority);
}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) {
  m_catalogue.modifyMountGroupArchiveMinFilesQueued(cliIdentity, name, minArchiveFilesQueued);
}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveBytesQueued) {
  m_catalogue.modifyMountGroupArchiveMinBytesQueued(cliIdentity, name, minArchiveBytesQueued);
}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) {
  m_catalogue.modifyMountGroupArchiveMinRequestAge(cliIdentity, name, minArchiveRequestAge);
}

//------------------------------------------------------------------------------
// modifyMountGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) {
  m_catalogue.modifyMountGroupRetrievePriority(cliIdentity, name, retrievePriority);
}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) {
  m_catalogue.modifyMountGroupRetrieveMinFilesQueued(cliIdentity, name, minRetrieveFilesQueued);
}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveBytesQueued) {
  m_catalogue.modifyMountGroupRetrieveMinBytesQueued(cliIdentity, name, minRetrieveBytesQueued);
}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) {
  m_catalogue.modifyMountGroupRetrieveMinRequestAge(cliIdentity, name, minRetrieveRequestAge);
}

//------------------------------------------------------------------------------
// modifyMountGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) {
  m_catalogue.modifyMountGroupMaxDrivesAllowed(cliIdentity, name, maxDrivesAllowed);
}

//------------------------------------------------------------------------------
// modifyMountGroupComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyMountGroupComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {
  m_catalogue.modifyMountGroupComment(cliIdentity, name, comment);
}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::Scheduler::createDedication(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &mountGroup,
        const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {
  m_catalogue.createDedication(cliIdentity, drivename, dedicationType, mountGroup, tag, vid, fromTimestamp, untilTimestamp, comment);
}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::Scheduler::deleteDedication(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename) {
  m_catalogue.deleteDedication(cliIdentity, drivename);
}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::Scheduler::getDedications(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_catalogue.getDedications(cliIdentity); 
}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {
  m_catalogue.modifyDedicationType(cliIdentity, drivename, dedicationType);
}

//------------------------------------------------------------------------------
// modifyDedicationMountGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &mountGroup) {
  m_catalogue.modifyDedicationMountGroup(cliIdentity, drivename, mountGroup);
}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) {
  m_catalogue.modifyDedicationTag(cliIdentity, drivename, tag);
}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) {
  m_catalogue.modifyDedicationVid(cliIdentity, drivename, vid);
}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) {
  m_catalogue.modifyDedicationFrom(cliIdentity, drivename, fromTimestamp);
}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) {
  m_catalogue.modifyDedicationUntil(cliIdentity, drivename, untilTimestamp);
}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) {
  m_catalogue.modifyDedicationComment(cliIdentity, drivename, comment);
}

//------------------------------------------------------------------------------
// repack
//------------------------------------------------------------------------------
void cta::Scheduler::repack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tag, const cta::common::dataStructures::RepackType) {
}

//------------------------------------------------------------------------------
// cancelRepack
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
}

//------------------------------------------------------------------------------
// getRepacks
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RepackInfo> cta::Scheduler::getRepacks(const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  return std::list<cta::common::dataStructures::RepackInfo>(); 
}

//------------------------------------------------------------------------------
// getRepack
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo cta::Scheduler::getRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  return cta::common::dataStructures::RepackInfo(); 
}

//------------------------------------------------------------------------------
// shrink
//------------------------------------------------------------------------------
void cta::Scheduler::shrink(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapepool) {
}

//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
void cta::Scheduler::verify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tag, const uint64_t numberOfFiles) {
}

//------------------------------------------------------------------------------
// cancelVerify
//------------------------------------------------------------------------------
void cta::Scheduler::cancelVerify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {

}

//------------------------------------------------------------------------------
// getVerifys
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::VerifyInfo> cta::Scheduler::getVerifys(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return std::list<cta::common::dataStructures::VerifyInfo>(); 
}

//------------------------------------------------------------------------------
// getVerify
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyInfo cta::Scheduler::getVerify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) const {
  return cta::common::dataStructures::VerifyInfo(); 
}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return m_catalogue.getArchiveFiles(cliIdentity, id, eosid, copynb, tapepool, vid, owner, group, storageclass, path); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::Scheduler::getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return m_catalogue.getArchiveFileSummary(cliIdentity, id, eosid, copynb, tapepool, vid, owner, group, storageclass, path);
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
cta::common::dataStructures::ReadTestResult cta::Scheduler::readTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t firstFSeq, const uint64_t lastFSeq, const bool checkChecksum, const std::string &output, const std::string &tag) const {
  return cta::common::dataStructures::ReadTestResult(); 
}

//------------------------------------------------------------------------------
// writeTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::writeTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const std::string &inputFile, const std::string &tag) const {
  return cta::common::dataStructures::WriteTestResult(); 
}

//------------------------------------------------------------------------------
// write_autoTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::write_autoTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t numberOfFiles, const uint64_t fileSize, const cta::common::dataStructures::TestSourceType testSourceType, const std::string &tag) const {
  return cta::common::dataStructures::WriteTestResult(); 
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::Scheduler::setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {

}

//------------------------------------------------------------------------------
// reconcile
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::reconcile(const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > cta::Scheduler::getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> >(); 
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob> cta::Scheduler::getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapePoolName) const {
  return std::list<cta::common::dataStructures::ArchiveJob>(); 
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > cta::Scheduler::getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  return std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> >(); 
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RetrieveJob> cta::Scheduler::getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) const {
  return std::list<cta::common::dataStructures::RetrieveJob>(); 
}

//------------------------------------------------------------------------------
// getDriveStates
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> cta::Scheduler::getDriveStates(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
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
