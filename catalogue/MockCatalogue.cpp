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
void cta::catalogue::MockCatalogue::createBootstrapAdminAndHostNoAuth(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &hostName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser>
  cta::catalogue::MockCatalogue::getAdminUsers(
  const SecurityIdentity &requester) const {

  return std::list<cta::AdminUser>();
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyAdminUserComment(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost>
  cta::catalogue::MockCatalogue::getAdminHosts(
  const SecurityIdentity &requester) const {
  return std::list<cta::AdminHost>();
}

//------------------------------------------------------------------------------
// modifyAdminHostComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyAdminHostComment(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
}


//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass>
  cta::catalogue::MockCatalogue::getStorageClasses(
  const SecurityIdentity &requester) const {
  return std::list<cta::StorageClass>();
}

//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyStorageClassNbCopies(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies) {
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyStorageClassComment(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}


//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const uint64_t minFilesQueued,
  const uint64_t minBytesQueued,
  const uint64_t minRequestAge,
  const uint16_t maxDrivesAllowed,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::catalogue::MockCatalogue::getTapePools(
  const SecurityIdentity &requester) const {
  return std::list<cta::TapePool>();
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolNbPartialTapes(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes) {
}

//------------------------------------------------------------------------------
// modifyTapePoolMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolMinFilesQueued(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minFilesQueued) {
}

//------------------------------------------------------------------------------
// modifyTapePoolMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolMinBytesQueued(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minBytesQueued) {
}

//------------------------------------------------------------------------------
// modifyTapePoolMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolMinRequestAge(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minRequestAge) {
}

//------------------------------------------------------------------------------
// modifyTapePoolMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolMaxDrivesAllowed(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t maxDrivesAllowed) {
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapePoolComment(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}


//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute> cta::catalogue::MockCatalogue::
  getArchiveRoutes(const SecurityIdentity &requester) const {
  return std::list<cta::ArchiveRoute>();
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyArchiveRouteTapePoolName(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName) {
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyArchiveRouteComment(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary>
  cta::catalogue::MockCatalogue::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return std::list<cta::LogicalLibrary>();
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyLogicalLibraryComment(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// getTape
//------------------------------------------------------------------------------
cta::Tape cta::catalogue::MockCatalogue::getTape(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  return cta::Tape();
}

//------------------------------------------------------------------------------
// getTapesByLogicalLibrary
//------------------------------------------------------------------------------
std::list<cta::Tape>
  cta::catalogue::MockCatalogue::getTapesByLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &logicalLibraryName) const {
  return std::list<cta::Tape>();
}

//------------------------------------------------------------------------------
// getTapesByPool
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::catalogue::MockCatalogue::getTapesByPool(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  return std::list<cta::Tape>();
}

//------------------------------------------------------------------------------
// getTapesByCapacity
//------------------------------------------------------------------------------
std::list<cta::Tape>
  cta::catalogue::MockCatalogue::getTapesByCapacity(
  const SecurityIdentity &requester,
  const uint64_t capacityInBytes) const {
  return std::list<cta::Tape>();
}

//------------------------------------------------------------------------------
// getAllTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::catalogue::MockCatalogue::getAllTapes(
  const SecurityIdentity &requester) const {
  return std::list<cta::Tape>();
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::reclaimTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeLogicalLibraryName(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName) {
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeTapePoolName(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &tapePoolName) {
}

//------------------------------------------------------------------------------
// modifyTapeCapacityInBytes
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeCapacityInBytes(
  const SecurityIdentity &requester,
  const std::string &vid,
  const uint64_t capacityInBytes) {
}

//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyTapeComment(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createRetrieveUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createRetrieveUser(
  const SecurityIdentity &requester,
  const std::string &username,
  const std::string &usergroup,
  const std::string &retrieveGroup,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteRetrieveUser
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteRetrieveUser(
  const SecurityIdentity &requester,
  const std::string &username,
  const std::string &usergroup) {
}

//------------------------------------------------------------------------------
// getRetrieveUsers
//------------------------------------------------------------------------------
std::list<cta::RetrieveUser>
  cta::catalogue::MockCatalogue::getRetrieveUsers(
  const SecurityIdentity &requester) const {
  return std::list<cta::RetrieveUser>();
}

//------------------------------------------------------------------------------
// modifyRetrieveUserRetrieveGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveUserRetrieveGroup(
  const SecurityIdentity &requester,
  const std::string &username,
  const std::string &usergroup,
  const std::string &retrieveGroup) {
}

//------------------------------------------------------------------------------
// modifyRetrieveUserComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveUserComment(
  const SecurityIdentity &requester,
  const std::string &username,
  const std::string &usergroup,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createRetrieveGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createRetrieveGroup(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minFilesQueued,
  const uint64_t minBytesQueued,
  const uint64_t minRequestAge,
  const uint16_t maxDrivesAllowed,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteRetrieveGroup
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteRetrieveGroup(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getRetrieveGroups
//------------------------------------------------------------------------------
std::list<cta::RetrieveGroup>
  cta::catalogue::MockCatalogue::getRetrieveGroups(
  const SecurityIdentity &requester) const {
  return std::list<cta::RetrieveGroup>();
}

//------------------------------------------------------------------------------
// modifyRetrieveGroupMinFilesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveGroupMinFilesQueued(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minFilesQueued) {
}

//------------------------------------------------------------------------------
// modifyRetrieveGroupMinBytesQueued
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveGroupMinBytesQueued(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minBytesQueued) {
}

//------------------------------------------------------------------------------
// modifyRetrieveGroupMinRequestAge
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveGroupMinRequestAge(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint64_t minRequestAge) {
}

//------------------------------------------------------------------------------
// modifyRetrieveGroupMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveGroupMaxDrivesAllowed(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t maxDrivesAllowed) {
}

//------------------------------------------------------------------------------
// modifyRetrieveGroupComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyRetrieveGroupComment(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// createDedication
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::createDedication(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const bool self,
  const bool readonly,
  const bool writeonly,
  const std::string &VO,
  const std::string &instanceName,
  const std::string &vid,
  const uint64_t fromTimestamp,
  const uint64_t untilTimestamp,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteDedication
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::deleteDedication(
  const SecurityIdentity &requester,
  const std::string &drivename) {
}

//------------------------------------------------------------------------------
// getDedications
//------------------------------------------------------------------------------
std::list<cta::Dedication>
  cta::catalogue::MockCatalogue::getDedications(
  const SecurityIdentity &requester) const {
  return std::list<cta::Dedication>();
}

//------------------------------------------------------------------------------
// modifyDedicationSelf
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationSelf(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const bool self) {
}

//------------------------------------------------------------------------------
// modifyDedicationReadonly
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationReadonly(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const bool readonly) {
}

//------------------------------------------------------------------------------
// modifyDedicationWriteonly
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationWriteonly(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const bool writeonly) {
}

//------------------------------------------------------------------------------
// modifyDedicationVO
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationVO(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const std::string &VO) {
}

//------------------------------------------------------------------------------
// modifyDedicationInstance
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationInstance(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const std::string &instanceName) {
}

//------------------------------------------------------------------------------
// modifyDedicationVid
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationVid(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// modifyDedicationFrom
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationFrom(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const uint64_t fromTimestamp) {
}

//------------------------------------------------------------------------------
// modifyDedicationUntil
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationUntil(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const uint64_t untilTimestamp) {
}

//------------------------------------------------------------------------------
// modifyDedicationComment
//------------------------------------------------------------------------------
void cta::catalogue::MockCatalogue::modifyDedicationComment(
  const SecurityIdentity &requester,
  const std::string &drivename,
  const std::string &comment) {
}

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
