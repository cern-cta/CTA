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
#include "common/exception/Exception.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::DummyCatalogue::DummyCatalogue():
  m_db(":memory:"),
  m_nextArchiveFileId(0) {

  char *zErrMsg = 0;
  if(SQLITE_OK != sqlite3_exec(m_db.getHandle(), "PRAGMA foreign_keys = ON;",
    0, 0, &zErrMsg)) {
    exception::Exception ex;
    ex.getMessage() << "Failed to turn on foreign key support in SQLite: " <<
      zErrMsg;
    sqlite3_free(zErrMsg);
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::DummyCatalogue::~DummyCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminUser> cta::catalogue::DummyCatalogue::getAdminUsers(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::AdminUser>();}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::AdminHost> cta::catalogue::DummyCatalogue::getAdminHosts(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::AdminHost>();}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::catalogue::DummyCatalogue::getStorageClasses(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::StorageClass>();}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies) {}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::TapePool> cta::catalogue::DummyCatalogue::getTapePools(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::TapePool>();}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes) {}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveRoute> cta::catalogue::DummyCatalogue::getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::ArchiveRoute>();}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) {}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::LogicalLibrary> cta::catalogue::DummyCatalogue::getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::LogicalLibrary>();}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Tape> cta::catalogue::DummyCatalogue::getTapes(const cta::common::dataStructures::SecurityIdentity &requester,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue) { return std::list<cta::common::dataStructures::Tape>();}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::labelTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::reclaimTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) {}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) {}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeBusy(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue) {}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeFull(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue) {}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue) {}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group) {}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::User> cta::catalogue::DummyCatalogue::getUsers(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::User>();}

//------------------------------------------------------------------------------
// modifyUserUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) {}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getUserGroups
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::UserGroup> cta::catalogue::DummyCatalogue::getUserGroups(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::UserGroup>();}

//------------------------------------------------------------------------------
// modifyUserGroupArchivePriority
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t retrievePriority) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) {}

//------------------------------------------------------------------------------
// modifyUserGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t maxDrivesAllowed) {}

//------------------------------------------------------------------------------
// modifyUserGroupComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyUserGroupComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::createDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::deleteDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::Dedication> cta::catalogue::DummyCatalogue::getDedications(const cta::common::dataStructures::SecurityIdentity &requester) const { return std::list<cta::common::dataStructures::Dedication>();}

//------------------------------------------------------------------------------
// modifyDedicationType
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) {}

//------------------------------------------------------------------------------
// modifyDedicationUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) {}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag) {}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment) {}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::catalogue::DummyCatalogue::getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return std::list<cta::common::dataStructures::ArchiveFile>(); 
}

//------------------------------------------------------------------------------
// getArchiveFileSummary
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFileSummary cta::catalogue::DummyCatalogue::getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &eosid,
        const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) {
  return cta::common::dataStructures::ArchiveFileSummary(); 
}
          
//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::setDriveStatus(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) {}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t cta::catalogue::DummyCatalogue::getNextArchiveFileId() {
  return m_nextArchiveFileId++;
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void cta::catalogue::DummyCatalogue::fileWrittenToTape(
  const cta::common::dataStructures::ArchiveRequest &archiveRequest,
  const cta::common::dataStructures::TapeFileLocation tapeFileLocation) {
}
