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
#include "catalogue/MockCatalogue.hpp"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>

//------------------------------------------------------------------------------
// deprecated constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler(NameServer &ns, SchedulerDatabase &db, RemoteNS &remoteNS) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Scheduler::~Scheduler() throw() {
}

//------------------------------------------------------------------------------
// queueArchiveRequest
//------------------------------------------------------------------------------
void cta::Scheduler::queueArchiveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ArchiveRequest &request) {

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
void cta::Scheduler::listStorageClassRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ListStorageClassRequest &request) {

}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::Scheduler::createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, 
        const std::string &comment) {

}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user) {

}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser> cta::Scheduler::getAdminUsers(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::AdminUser>();
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::createAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::Scheduler::deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName) {

}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::Scheduler::getAdminHosts(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::AdminHost>(); 
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {

}


//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::createStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {

}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::getStorageClasses(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::StorageClass>(); 
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies) {

}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::createTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {

}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::Scheduler::getTapePools(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::TapePool>(); 
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes) {

}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName,
        const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb) {

}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::Scheduler::getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::ArchiveRoute>(); 
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {

}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::Scheduler::deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {

}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::Scheduler::getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::LogicalLibrary>(); 
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::Scheduler::createTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::Scheduler::deleteTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {

}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::Scheduler::getTapes(const cta::common::dataStructures::SecurityIdentity &requester,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue) {
  return std::list<cta::common::dataStructures::Tape>(); 
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

}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) {

}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) {

}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) {

}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeBusy(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue) {

}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeFull(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue) {

}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue) {

}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::Scheduler::createUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup,
        const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::Scheduler::deleteUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group) {

}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::Scheduler::getUsers(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::User>(); 
}

//------------------------------------------------------------------------------
// modifyUserUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) {

}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::createUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued,
        const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
        const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::deleteUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {

}

//------------------------------------------------------------------------------
// getUserGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::UserGroup> cta::Scheduler::getUserGroups(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::UserGroup>(); 
}

//------------------------------------------------------------------------------
// modifyUserGroupArchivePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority) {

}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) {

}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) {

}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) {

}

//------------------------------------------------------------------------------
// modifyUserGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t retrievePriority) {

}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) {

}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) {

}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) {

}

//------------------------------------------------------------------------------
// modifyUserGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t maxDrivesAllowed) {

}

//------------------------------------------------------------------------------
// modifyUserGroupComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyUserGroupComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {

}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::Scheduler::createDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
        const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {

}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::Scheduler::deleteDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename) {

}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::Scheduler::getDedications(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::list<cta::common::dataStructures::Dedication>(); 
}

//------------------------------------------------------------------------------
// modifyDedicationReadonly
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationReadonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly) {

}

//------------------------------------------------------------------------------
// modifyDedicationWriteonly
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationWriteonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool writeonly) {

}

//------------------------------------------------------------------------------
// modifyDedicationUserGroup
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) {

}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag) {

}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid) {

}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) {

}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) {

}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::Scheduler::modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment) {

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
void cta::Scheduler::verify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tag, const cta::common::dataStructures::VerifyType) {

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
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool,
        const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::Scheduler::getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool, 
          const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}

//------------------------------------------------------------------------------
// readTest
//------------------------------------------------------------------------------
cta::common::dataStructures::ReadTestResult cta::Scheduler::readTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid,
        const uint64_t firstFSeq, const uint64_t lastFSeq, const bool checkChecksum, const uint64_t retriesPerFile, const std::string &outputDir, const bool redirectToDevNull, const std::string &tag) const {
  return cta::common::dataStructures::ReadTestResult(); 
}

//------------------------------------------------------------------------------
// writeTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::writeTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid,
        const uint64_t numberOfFiles, const uint64_t fileSize, const bool randomSize, const bool devZero, const bool devURandom, const std::list<std::string> &inputFiles, const std::string &tag) const {
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
std::map<cta::common::dataStructures::TapePool, std::list<cta::common::dataStructures::ArchiveJob> > cta::Scheduler::getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::map<cta::common::dataStructures::TapePool, std::list<cta::common::dataStructures::ArchiveJob> >(); 
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
std::map<cta::common::dataStructures::Tape, std::list<cta::common::dataStructures::RetrieveJob> > cta::Scheduler::getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const {
  return std::map<cta::common::dataStructures::Tape, std::list<cta::common::dataStructures::RetrieveJob> >(); 
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
std::unique_ptr<cta::common::dataStructures::TapeMount> cta::Scheduler::getNextMount(const std::string &logicalLibraryName, const std::string &driveName) {
  return std::unique_ptr<cta::common::dataStructures::TapeMount>();
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::Scheduler::_old_getNextMount(const std::string &logicalLibraryName, const std::string & driveName) {
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
  return std::unique_ptr<TapeMount>();
}
