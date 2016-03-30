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
  SchedulerDatabase &db): m_catalogue(catalogue), m_db(db) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Scheduler::~Scheduler() throw() {
}

//------------------------------------------------------------------------------
// authorizeCliIdentity
//------------------------------------------------------------------------------
void cta::Scheduler::authorizeCliIdentity(const cta::common::dataStructures::SecurityIdentity &cliIdentity){
  m_catalogue.isAdmin(cliIdentity);
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
// labelTape
//------------------------------------------------------------------------------
void cta::Scheduler::labelTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {
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
// setTapeLbp
//------------------------------------------------------------------------------
void cta::Scheduler::setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) {
  m_catalogue.setTapeLbp(cliIdentity, vid, lbpValue);
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
