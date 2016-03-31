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

#include "catalogue/DummyCatalogue.hpp"


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::DummyCatalogue::~DummyCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createAdminUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteAdminUser(const cta::common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser> cta::catalogue::DummyCatalogue::getAdminUsers() const { return std::list<cta::common::dataStructures::AdminUser>();}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createAdminHost(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteAdminHost(const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::catalogue::DummyCatalogue::getAdminHosts() const { return std::list<cta::common::dataStructures::AdminHost>();}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteStorageClass(const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::catalogue::DummyCatalogue::getStorageClasses() const { return std::list<cta::common::dataStructures::StorageClass>();}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) {}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteTapePool(const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::catalogue::DummyCatalogue::getTapePools() const { return std::list<cta::common::dataStructures::TapePool>();}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) {}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapePoolEncryption(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) {}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::catalogue::DummyCatalogue::getArchiveRoutes() const { return std::list<cta::common::dataStructures::ArchiveRoute>();}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteLogicalLibrary(const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::catalogue::DummyCatalogue::getLogicalLibraries() const { return std::list<cta::common::dataStructures::LogicalLibrary>();}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteTape(const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::catalogue::DummyCatalogue::getTapes(const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue) { return std::list<cta::common::dataStructures::Tape>();}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::reclaimTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) {}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) {}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKey
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeEncryptionKey(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) {}

//------------------------------------------------------------------------------
// modifyTapeLabelLog
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeLabelLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastWrittenLog
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeLastWrittenLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// modifyTapeLastReadLog
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeLastReadLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) {}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeBusy(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) {}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeFull(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) {}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) {}

//------------------------------------------------------------------------------
// setTapeLbp
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) {}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteUser(const std::string &name, const std::string &group) {}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::catalogue::DummyCatalogue::getUsers() const { return std::list<cta::common::dataStructures::User>();}

//------------------------------------------------------------------------------
// modifyUserMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup) {}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &comment) {}

//------------------------------------------------------------------------------
// createMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteMountGroup(const std::string &name) {}

//------------------------------------------------------------------------------
// getMountGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::MountGroup> cta::catalogue::DummyCatalogue::getMountGroups() const { return std::list<cta::common::dataStructures::MountGroup>();}

//------------------------------------------------------------------------------
// modifyMountGroupArchivePriority
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyMountGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) {}

//------------------------------------------------------------------------------
// modifyMountGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) {}

//------------------------------------------------------------------------------
// modifyMountGroupComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyMountGroupComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createDedication(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &mountGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteDedication(const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::catalogue::DummyCatalogue::getDedications() const { return std::list<cta::common::dataStructures::Dedication>();}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {}

//------------------------------------------------------------------------------
// modifyDedicationMountGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &mountGroup) {}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) {}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) {}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::catalogue::DummyCatalogue::getArchiveFiles(const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::catalogue::DummyCatalogue::getArchiveFileSummary(const uint64_t id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile cta::catalogue::DummyCatalogue::getArchiveFileById(const uint64_t id){
  return cta::common::dataStructures::ArchiveFile();
}
          
//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::fileWrittenToTape(
  const cta::common::dataStructures::ArchiveRequest &archiveRequest,
  const cta::common::dataStructures::TapeFileLocation &tapeFileLocation) {
}

//------------------------------------------------------------------------------
// prepareForNewFile 
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileQueueCriteria
    cta::catalogue::DummyCatalogue::prepareForNewFile(
    const std::string &storageClass, const std::string &user) {
  return common::dataStructures::ArchiveFileQueueCriteria();
} 

//------------------------------------------------------------------------------
// getTapeCopyToPoolMap
//------------------------------------------------------------------------------  
cta::common::dataStructures::TapeCopyToPoolMap cta::catalogue::DummyCatalogue::
  getTapeCopyToPoolMap(const std::string &storageClass) const {
  return cta::common::dataStructures::TapeCopyToPoolMap();
}

//------------------------------------------------------------------------------
// getArchiveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::DummyCatalogue::
  getArchiveMountPolicy(const std::string &user) const {
  return cta::common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// getRetrieveMountPolicy
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy cta::catalogue::DummyCatalogue::
  getRetrieveMountPolicy(const std::string &user) const {
  return cta::common::dataStructures::MountPolicy();
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool cta::catalogue::DummyCatalogue::isAdmin(
  const common::dataStructures::SecurityIdentity &cliIdentity) const {
  return true;
}
