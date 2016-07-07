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


#include "Scheduler.hpp"
#include "ArchiveMount.hpp"
#include "RetrieveMount.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/NonRetryableError.hpp"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <random>
#include <chrono>

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
  auto catalogueInfo = m_catalogue.prepareForNewFile(request.instance, request.storageClass, request.requester);
  m_db.queueArchive(request, catalogueInfo);
  return catalogueInfo.fileId;
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
void cta::Scheduler::queueRetrieve(
  const cta::common::dataStructures::SecurityIdentity &cliIdentity,
  const cta::common::dataStructures::RetrieveRequest &request) {
  // Get the 
  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue.prepareToRetrieveFile(request.archiveFileID, request.requester);
  // Get the statuses of the tapes on which we have files.
  std::set<std::string> vids;
  for (auto & tf: queueCriteria.archiveFile.tapeFiles) {
    vids.insert(tf.second.vid);
  }
  auto tapeStatuses = m_catalogue.getTapesByVid(vids);
  // Filter out the tapes with are disabled
  for (auto & t: tapeStatuses) {
    if (t.second.disabled)
      vids.erase(t.second.vid);
  }
  if (vids.empty())
    throw cta::exception::NonRetryableError("In Scheduler::queueRetrieve(): all copies are on disabled tapes");
  // Get the statistics for the potential tapes on which we will retrieve.
  auto stats=m_db.getRetrieveQueueStatistics(queueCriteria, vids);
  // Sort the potential queues.
  stats.sort(SchedulerDatabase::RetrieveQueueStatistics::leftGreaterThanRight);
  // If there are several equivalent entries, choose randomly among them.
  // First element will always be selected.
  std::set<std::string> candidateVids;
  for (auto & s: stats) {
    if (!(s<stats.front()) && !(s>stats.front()))
      candidateVids.insert(s.vid);
  }
  if (candidateVids.empty())
    throw cta::exception::Exception("In Scheduler::queueRetrieve(): failed to sort and select candidate VIDs");
  // We need to get a random number [0, candidateVids.size() -1]
  std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<size_t> distribution(0, candidateVids.size() -1);
  size_t index=distribution(dre);
  auto it=candidateVids.cbegin();
  std::advance(it, index);
  std::string selectedVid=*it;
  m_db.queueRetrieve(request, queueCriteria, selectedVid);
}

//------------------------------------------------------------------------------
// deleteArchive
//------------------------------------------------------------------------------
void cta::Scheduler::deleteArchive(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::DeleteArchiveRequest &request) {
  m_catalogue.deleteArchiveFile(request.archiveFileID);
  m_db.deleteArchiveRequest(cliIdentity, request.archiveFileID);
}

//------------------------------------------------------------------------------
// cancelRetrieve
//------------------------------------------------------------------------------
void cta::Scheduler::cancelRetrieve(const cta::common::dataStructures::SecurityIdentity &cliIdentity,  const cta::common::dataStructures::CancelRetrieveRequest &request) {
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
  // In order to decide the next mount to do, we have to take a global lock on 
  // the scheduling, retrieve a list of all running mounts, queues sizes for 
  // tapes and tape pools, order the candidates by priority
  // below threshold, and pick one at a time, we then attempt to get a tape 
  // from the catalogue (for the archive mounts), and walk the list until we
  // mount or find nothing to do.
  // We then skip to the next candidate, until we find a suitable one and
  // return the mount, or exhaust all of them an 
  // Many steps for this logic are not specific for the database and are hence
  // implemented in the scheduler itself.
  // First, get the mount-related info from the DB
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfo();
  __attribute__((unused)) SchedulerDatabase::TapeMountDecisionInfo & debugMountInfo = *mountInfo;
  
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
    if (m->bytesQueued / (1 + existingMounts) >= m_minBytesToWarrantAMount)
      mountPassesACriteria = true;
    if (m->filesQueued / (1 + existingMounts) >= m_minFilesToWarrantAMount)
      mountPassesACriteria = true;
    if (!existingMounts && ((time(NULL) - m->oldestJobStartTime) > m->minArchiveRequestAge))
      mountPassesACriteria = true;
    if (!mountPassesACriteria || existingMounts >= m->maxDrivesAllowed) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      // populate the mount with a weight 
      m->ratioOfMountQuotaUsed = 1.0L * existingMounts / m->maxDrivesAllowed;
      m++;
   }
  }
  
  // We can now sort the potential mounts in decreasing priority order. 
  // The ordering is defined in operator <.
  // We want the result in descending order or priority so we reverse the vector
  std::sort(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  std::reverse(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  
  // Find out if we have any potential archive mount in the list. If so, get the
  // list of tapes from the catalogue.
  std::list<cta::catalogue::TapeForWriting> tapeList;
  if (std::count_if(
        mountInfo->potentialMounts.cbegin(), mountInfo->potentialMounts.cend(), 
        [](decltype(*mountInfo->potentialMounts.cbegin())& m){ return m.type == cta::MountType::ARCHIVE; } )) {
    tapeList = m_catalogue.getTapesForWriting(logicalLibraryName);
  }
  
  // We can now simply iterate on the candidates until we manage to create a
  // mount for one of them
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (m->type==cta::MountType::ARCHIVE) {
      // We need to find a tape for archiving. It should be both in the right 
      // tape pool and in the drive's logical library
      // The first tape matching will go for a prototype.
      // TODO: improve to reuse already partially written tapes and randomization
      for (auto & t: tapeList) {
        if (t.tapePool == m->tapePool) {
          // We have our tape. Try to create the session. Prepare a return value
          // for it.
          std::unique_ptr<ArchiveMount> internalRet(new ArchiveMount(m_catalogue));
          // Get the db side of the session
          try {
            internalRet->m_dbMount.reset(mountInfo->createArchiveMount(t,
                driveName, 
                logicalLibraryName, 
                cta::utils::getShortHostname(), 
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
      auto drives = m_db.getDriveStates();
      if (!std::count_if(drives.cbegin(), drives.cend(), [&](decltype(*drives.cbegin()) & d){ return d.currentVid == m->vid; })) {
        try {
          // create the mount, and populate its DB side.
          std::unique_ptr<RetrieveMount> internalRet (
            new RetrieveMount(mountInfo->createRetrieveMount(m->vid, 
              m->tapePool,
              driveName,
              logicalLibraryName, 
              cta::utils::getShortHostname(), 
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
    } else {
      throw std::runtime_error("In Scheduler::getNextMount unexpected mount type");
    }
  }
  return std::unique_ptr<cta::TapeMount>();
}
