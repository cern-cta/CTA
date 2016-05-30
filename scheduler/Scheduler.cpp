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
// constructor
//------------------------------------------------------------------------------
cta::Scheduler::Scheduler(
  catalogue::Catalogue &catalogue,
  SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount): 
    m_catalogue(catalogue), m_db(db), m_minFilesToWarrantAMount(minFilesToWarrantAMount), 
    m_minBytesToWarrantAMount(minBytesToWarrantAMount) {}

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
// queueArchive
//------------------------------------------------------------------------------
uint64_t cta::Scheduler::queueArchive(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ArchiveRequest &request) {
  auto catalogueInfo = m_catalogue.prepareForNewFile(request.storageClass, request.requester);
  m_db.queue(request, catalogueInfo);
  return catalogueInfo.fileId;
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
void cta::Scheduler::queueRetrieve(
  const cta::common::dataStructures::SecurityIdentity &cliIdentity,
  const cta::common::dataStructures::RetrieveRequest &request) {
  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue.prepareToRetrieveFile(request.archiveFileID, request.requester);
  m_db.queue(request, queueCriteria);
}

//------------------------------------------------------------------------------
// deleteArchive
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchive(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::DeleteArchiveRequest &request) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// cancelRetrieve
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRetrieve(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::CancelRetrieveRequest &request) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// updateFileInfo
//------------------------------------------------------------------------------
void cta::Scheduler::updateFileInfo(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UpdateFileInfoRequest &request) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// updateFileStorageClass
//------------------------------------------------------------------------------
void cta::Scheduler::updateFileStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UpdateFileStorageClassRequest &request) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// listStorageClass
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::StorageClass> cta::Scheduler::listStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ListStorageClassRequest &request) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void cta::Scheduler::labelTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force, const bool lbp, const std::string &tag) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
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
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// cancelRepack
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getRepacks
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::RepackInfo> cta::Scheduler::getRepacks(const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getRepack
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo cta::Scheduler::getRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__); 
}

//------------------------------------------------------------------------------
// shrink
//------------------------------------------------------------------------------
void cta::Scheduler::shrink(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapepool) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
void cta::Scheduler::verify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tag, const uint64_t numberOfFiles) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// cancelVerify
//------------------------------------------------------------------------------
void cta::Scheduler::cancelVerify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
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
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// readTest
//------------------------------------------------------------------------------
cta::common::dataStructures::ReadTestResult cta::Scheduler::readTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t firstFSeq, const uint64_t lastFSeq, const bool checkChecksum, const std::string &output, const std::string &tag) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// writeTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::writeTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const std::string &inputFile, const std::string &tag) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// write_autoTest
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult cta::Scheduler::write_autoTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t numberOfFiles, const uint64_t fileSize, const cta::common::dataStructures::TestSourceType testSourceType, const std::string &tag) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void cta::Scheduler::setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// reconcile
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveFile> cta::Scheduler::reconcile(const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > cta::Scheduler::getPendingArchiveJobs() const {
  return m_db.getArchiveJobs();
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::ArchiveJob> cta::Scheduler::getPendingArchiveJobs(const std::string &tapePoolName) const {
  return m_db.getArchiveJobs(tapePoolName);
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > cta::Scheduler::getPendingRetrieveJobs() const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------

std::list<cta::common::dataStructures::RetrieveJob> cta::Scheduler::getPendingRetrieveJobs(const std::string& vid) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getDriveStates
//------------------------------------------------------------------------------
std::list<cta::common::dataStructures::DriveState> cta::Scheduler::getDriveStates(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const {
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<cta::TapeMount> cta::Scheduler::getNextMount(const std::string &logicalLibraryName, const std::string &driveName) {
  /*
  // In order to decide the next mount to do, we have to take a global lock on 
  // the scheduling, retrieve a list of all running mounts, queues sizes for 
  // tapes and tape pools, filter the tapes which are actually accessible to
  // this drive (by library and dedication), order the candidates by priority
  // below threshold, and pick one at a time. In addition, for archives, we
  // might not find a suitable tape (by library and dedication). In such a case,
  // we should find out if no tape at all is available, and log an error if 
  // so.
  // We then skip to the next candidate, until we find a suitable one and
  // return the mount, or exhaust all of them an 
  // Many steps for this logic are not specific for the database and are hence
  // implemented in the scheduler itself.
  // First, get the mount-related info from the DB
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfo();
  
  // We should now filter the potential mounts to keep only the ones we are
  // compatible with (match the logical library for retrieves).
  // We also only want the potential mounts for which we still have 
  // We cannot filter the archives yet
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    if (m->type == MountType::RETRIEVE && m->logicalLibrary != logicalLibraryName) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      m++;
    }
  }
  
  // With the existing mount list, we can now populate the potential mount list
  // with the per tape pool existing mount statistics.
  typedef std::pair<std::string, cta::MountType::Enum> tpType;
  std::map<tpType, uint32_t> existingMountsSummary;
  for (auto em=mountInfo->existingMounts.begin(); em!=mountInfo->existingMounts.end(); em++) {
    try {
      existingMountsSummary.at(tpType(em->tapePool, em->type))++;
    } catch (std::out_of_range &) {
      existingMountsSummary[tpType(em->tapePool, em->type)] = 1;
    }
  }
  
  // We can now filter out the potential mounts for which their mount criteria
  // is already met, filter out the potential mounts for which the maximum mount
  // quota is already reached, and weight the remaining by how much of their quota 
  // is reached
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    // Get summary data
    uint32_t existingMounts;
    try {
      existingMounts = existingMountsSummary.at(tpType(m->tapePool, m->type));
    } catch (std::out_of_range &) {
      existingMounts = 0;
    } 
    bool mountPassesACriteria = false;
    if (m->bytesQueued / (1 + existingMounts) >= m->mountCriteria.maxBytesQueued)
      mountPassesACriteria = true;
    if (m->filesQueued / (1 + existingMounts) >= m->mountCriteria.maxFilesQueued)
      mountPassesACriteria = true;
    if (!existingMounts && ((time(NULL) - m->oldestJobStartTime) > (int64_t)m->mountCriteria.maxAge))
      mountPassesACriteria = true;
    if (!mountPassesACriteria || existingMounts >= m->mountCriteria.quota) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      // populate the mount with a weight 
      m->ratioOfMountQuotaUsed = 1.0L * existingMounts / m->mountCriteria.quota;
      m++;
   }
  }
  
  // We can now sort the potential mounts in decreasing priority order. 
  // The ordering is defined in operator <.
  // We want the result in descending order or priority so we reverse the vector
  std::sort(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  std::reverse(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  
  // We can now simply iterate on the candidates until we manage to create a
  // mount for one of them
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (m->type==cta::MountType::ARCHIVE) {
      // We need to find a tape for archiving. It should be both in the right 
      // tape pool and in the drive's logical library
      auto tapesList = m_db.getTapes();
      // The first tape matching will go for a prototype.
      // TODO: improve to reuse already partially written tapes
      for (auto t=tapesList.begin(); t!=tapesList.end(); t++) {
        if (t->logicalLibraryName == logicalLibraryName &&
            t->tapePoolName == m->tapePool &&
            t->status.availableToWrite()) {
          // We have our tape. Try to create the session. Prepare a return value
          // for it.
          std::unique_ptr<ArchiveMount> internalRet(new ArchiveMount(m_ns));
          // Get the db side of the session
          try {
            internalRet->m_dbMount.reset(mountInfo->createArchiveMount(t->vid,
                t->tapePoolName, 
                driveName, 
                logicalLibraryName, 
                Utils::getShortHostname(), 
                time(NULL)).release());
            internalRet->m_sessionRunning = true;
            internalRet->setDriveStatus(cta::common::DriveStatus::Starting);
            return std::unique_ptr<TapeMount> (internalRet.release());
          } catch (cta::exception::Exception & ex) {
            continue;
          }
        }
      }
    } else if (m->type==cta::MountType::RETRIEVE) {
      // We know the tape we intend to mount. We have to validate the tape is 
      // actually available to read, and pass on it if no.
      auto tapesList = m_db.getTapes();
      for (auto t=tapesList.begin(); t!=tapesList.end(); t++) {
        if (t->vid == m->vid && t->status.availableToRead()) {
          try {
            // create the mount, and populate its DB side.
            std::unique_ptr<RetrieveMount> internalRet (
              new RetrieveMount(mountInfo->createRetrieveMount(t->vid, 
                t->tapePoolName,
                driveName,
                logicalLibraryName, 
                Utils::getShortHostname(), 
                time(NULL))));
            internalRet->m_sessionRunning = true;
            internalRet->m_diskRunning = true;
            internalRet->m_tapeRunning = true;
            internalRet->setDriveStatus(cta::common::DriveStatus::Starting);
            return std::unique_ptr<TapeMount> (internalRet.release()); 
         } catch (cta::exception::Exception & ex) {
           std::string debug=ex.getMessageValue();
           continue;
         }
        }
      }
    } else {
      throw std::runtime_error("In Scheduler::getNextMount unexpected mount type");
    }
  }
  */
  throw cta::exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}
