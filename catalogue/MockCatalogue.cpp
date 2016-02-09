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

#include "catalogue/MockCatalogue.hpp"
#include "common/exception/Exception.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::MockCatalogue::MockCatalogue():
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
cta::catalogue::MockCatalogue::~MockCatalogue() {
}

//------------------------------------------------------------------------------
// createBootstrapAdminAndHostNoAuth
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createBootstrapAdminAndHostNoAuth(const SecurityIdentity &requester, const UserIdentity &user, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createAdminUser(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteAdminUser(const SecurityIdentity &requester, const UserIdentity &user) {}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::catalogue::MockCatalogue::getAdminUsers(const SecurityIdentity &requester) const { return std::list<cta::AdminUser>();}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyAdminUserComment(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment) {}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createAdminHost(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteAdminHost(const SecurityIdentity &requester, const std::string &hostName) {}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::catalogue::MockCatalogue::getAdminHosts(const SecurityIdentity &requester) const { return std::list<cta::AdminHost>();}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyAdminHostComment(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment) {}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteStorageClass(const SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::catalogue::MockCatalogue::getStorageClasses(const SecurityIdentity &requester) const { return std::list<cta::StorageClass>();}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyStorageClassNbCopies(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies) {}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyStorageClassComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createTapePool(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteTapePool(const SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::catalogue::MockCatalogue::getTapePools(const SecurityIdentity &requester) const { return std::list<cta::TapePool>();}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolNbPartialTapes(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes) {}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb) {}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute> cta::catalogue::MockCatalogue::getArchiveRoutes(const SecurityIdentity &requester) const { return std::list<cta::ArchiveRoute>();}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyArchiveRouteTapePoolName(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyArchiveRouteComment(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment) {}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::catalogue::MockCatalogue::getLogicalLibraries(const SecurityIdentity &requester) const { return std::list<cta::LogicalLibrary>();}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyLogicalLibraryComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteTape(const SecurityIdentity &requester, const std::string &vid) {}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::catalogue::MockCatalogue::getTapes(const SecurityIdentity &requester, const std::map<std::string, std::string> &where) { return std::list<cta::Tape>();}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::labelTape(const SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::reclaimTape(const SecurityIdentity &requester, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeLogicalLibraryName(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) {}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeTapePoolName(const SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) {}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeCapacityInBytes(const SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) {}

//------------------------------------------------------------------------------
// setTapeBusy
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::setTapeBusy(const SecurityIdentity &requester, const std::string &vid, const bool busyValue) {}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::setTapeFull(const SecurityIdentity &requester, const std::string &vid, const bool fullValue) {}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::setTapeDisabled(const SecurityIdentity &requester, const std::string &vid, const bool disabledValue) {}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeComment(const SecurityIdentity &requester, const std::string &vid, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createUser(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteUser(const SecurityIdentity &requester, const std::string &name, const std::string &group) {}

//------------------------------------------------------------------------------
// getUsers
//------------------------------------------------------------------------------
std::list<cta::User> cta::catalogue::MockCatalogue::getUsers(const SecurityIdentity &requester) const { return std::list<cta::User>();}

//------------------------------------------------------------------------------
// modifyUserUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserUserGroup(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) {}

//------------------------------------------------------------------------------
// modifyUserComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserComment(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) {}

//------------------------------------------------------------------------------
// createUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createUserGroup(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteUserGroup(const SecurityIdentity &requester, const std::string &name) {}

//------------------------------------------------------------------------------
// getUserGroups
//------------------------------------------------------------------------------
std::list<cta::UserGroup> cta::catalogue::MockCatalogue::getUserGroups(const SecurityIdentity &requester) const { return std::list<cta::UserGroup>();}

//------------------------------------------------------------------------------
// modifyUserGroupArchivePriority
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupArchivePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupArchiveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupArchiveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupArchiveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupArchiveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrievePriority
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupRetrievePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupRetrieveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupRetrieveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) {}

//------------------------------------------------------------------------------
// modifyUserGroupRetrieveMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupRetrieveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) {}

//------------------------------------------------------------------------------
// modifyUserGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed) {}

//------------------------------------------------------------------------------
// modifyUserGroupComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyUserGroupComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) {}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createDedication(const SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) {}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteDedication(const SecurityIdentity &requester, const std::string &drivename) {}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::Dedication> cta::catalogue::MockCatalogue::getDedications(const SecurityIdentity &requester) const { return std::list<cta::Dedication>();}

//------------------------------------------------------------------------------
// modifyDedicationReadonly
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationReadonly(const SecurityIdentity &requester, const std::string &drivename, const bool readonly) {}

//------------------------------------------------------------------------------
// modifyDedicationWriteonly
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationWriteonly(const SecurityIdentity &requester, const std::string &drivename, const bool writeonly) {}

//------------------------------------------------------------------------------
// modifyDedicationUserGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationUserGroup(const SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) {}

//------------------------------------------------------------------------------
// modifyDedicationTag
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationTag(const SecurityIdentity &requester, const std::string &drivename, const std::string &tag) {}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationVid(const SecurityIdentity &requester, const std::string &drivename, const std::string &vid) {}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationFrom(const SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationUntil(const SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) {}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationComment(const SecurityIdentity &requester, const std::string &drivename, const std::string &comment) {}

//------------------------------------------------------------------------------
// getArchiveFiles
//------------------------------------------------------------------------------
std::list<cta::ArchiveFile> cta::catalogue::MockCatalogue::getArchiveFiles(const SecurityIdentity &requester, const std::map<std::string, std::string> &where) { return std::list<cta::ArchiveFile>();}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::setDriveStatus(const SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) {}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t cta::catalogue::MockCatalogue::getNextArchiveFileId() {
  return m_nextArchiveFileId++;
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::fileWrittenToTape(
  const ArchiveRequest &archiveRequest,
  const std::string vid,
  const uint64_t fSeq,
  const uint64_t blockId) {
}
