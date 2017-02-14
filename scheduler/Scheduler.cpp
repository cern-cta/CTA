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
#include "common/Timer.hpp"
#include "common/exception/NonRetryableError.hpp"
#include "common/exception/UserError.hpp"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <random>
#include <chrono>

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Scheduler::Scheduler(
  catalogue::Catalogue &catalogue,
  SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount): 
    m_catalogue(catalogue), m_db(db), m_minFilesToWarrantAMount(minFilesToWarrantAMount), 
    m_minBytesToWarrantAMount(minBytesToWarrantAMount) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Scheduler::~Scheduler() throw() { }

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void Scheduler::ping() {
  m_db.ping();
  m_catalogue.ping();
}

//------------------------------------------------------------------------------
// authorizeAdmin
//------------------------------------------------------------------------------
void Scheduler::authorizeAdmin(const common::dataStructures::SecurityIdentity &cliIdentity){
  if(!(m_catalogue.isAdmin(cliIdentity))) {
    std::stringstream msg;
    msg << "User: " << cliIdentity.username << " on host: " << cliIdentity.host << " is not authorized to execute CTA admin commands";
    throw exception::UserError(msg.str());
  }
}

//------------------------------------------------------------------------------
// queueArchive
//------------------------------------------------------------------------------
uint64_t Scheduler::queueArchive(const std::string &instanceName, const common::dataStructures::ArchiveRequest &request,
    log::LogContext & lc) {
  cta::utils::Timer t;
  using utils::postEllipsis;
  using utils::midEllipsis;
  auto catalogueInfo = m_catalogue.prepareForNewFile(instanceName, request.storageClass, request.requester);
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  m_db.queueArchive(instanceName, request, catalogueInfo, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("instanceName", instanceName)
     .add("storageClass", request.storageClass)
     .add("diskFileID", request.diskFileID)
     .add("fileSize", request.fileSize)
     .add("fileId", catalogueInfo.fileId);
  for (auto & ctp: catalogueInfo.copyToPoolMap) {
    std::stringstream tp;
    tp << "tapePool" << ctp.first;
    spc.add(tp.str(), ctp.second);
  }
  spc.add("policyName", catalogueInfo.mountPolicy.name)
     .add("policyArchiveMinAge", catalogueInfo.mountPolicy.archiveMinRequestAge)
     .add("policyArchivePriority", catalogueInfo.mountPolicy.archivePriority)
     .add("policyMaxDrives", catalogueInfo.mountPolicy.maxDrivesAllowed)
     .add("diskFilePath", request.diskFileInfo.path)
     .add("diskFileOwner", request.diskFileInfo.owner)
     .add("diskFileGroup", request.diskFileInfo.group)
     .add("diskFileRecoveryBlob", postEllipsis(request.diskFileInfo.recoveryBlob, 20))
     .add("checksumValue", request.checksumValue)
     .add("checksumType", request.checksumType)
     .add("archiveReportURL", midEllipsis(request.archiveReportURL, 50, 15))
     .add("creationHost", request.creationLog.host)
     .add("creationTime", request.creationLog.time)
     .add("creationUser", request.creationLog.username)
     .add("requesterName", request.requester.name)
     .add("requesterGroup", request.requester.group)
     .add("srcURL", midEllipsis(request.srcURL, 50, 15))
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "Queued archive request");
  return catalogueInfo.fileId;
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
void Scheduler::queueRetrieve(
  const std::string &instanceName,
  const common::dataStructures::RetrieveRequest &request,
  log::LogContext & lc) {
  using utils::postEllipsis;
  using utils::midEllipsis;
  utils::Timer t;
  // Get the 
  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue.prepareToRetrieveFile(instanceName, request.archiveFileID, request.requester);
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
    throw exception::NonRetryableError("In Scheduler::queueRetrieve(): all copies are on disabled tapes");
  auto catalogueTime = t.secs(utils::Timer::resetCounter);
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
    throw exception::Exception("In Scheduler::queueRetrieve(): failed to sort and select candidate VIDs");
  // We need to get a random number [0, candidateVids.size() -1]
  std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<size_t> distribution(0, candidateVids.size() -1);
  size_t index=distribution(dre);
  auto it=candidateVids.cbegin();
  std::advance(it, index);
  std::string selectedVid=*it;
  m_db.queueRetrieve(request, queueCriteria, selectedVid);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("archiveFileID", request.archiveFileID)
     .add("instanceName", instanceName)
     .add("diskFilePath", request.diskFileInfo.path)
     .add("diskFileOwner", request.diskFileInfo.owner)
     .add("diskFileGroup", request.diskFileInfo.group)
     .add("diskFileRecoveryBlob", postEllipsis(request.diskFileInfo.recoveryBlob, 20))
     .add("dstURL", request.dstURL)
     .add("creationHost", request.entryLog.host)
     .add("creationTime", request.entryLog.time)
     .add("creationUser", request.entryLog.username)
     .add("requesterName", request.requester.name)
     .add("requesterGroup", request.requester.group)
     .add("criteriaArchiveFileId", queueCriteria.archiveFile.archiveFileID)
     .add("criteriaChecksumType", queueCriteria.archiveFile.checksumType)
     .add("criteriaChecksumValue", queueCriteria.archiveFile.checksumValue)
     .add("criteriaCreationTime", queueCriteria.archiveFile.creationTime)
     .add("criteriaDiskFileId", queueCriteria.archiveFile.diskFileId)
     .add("criteriaDiskFilePath", queueCriteria.archiveFile.diskFileInfo.path)
     .add("criteriaDiskFileOwner", queueCriteria.archiveFile.diskFileInfo.owner)
     .add("criteriaDiskRecoveryBlob", postEllipsis(queueCriteria.archiveFile.diskFileInfo.recoveryBlob, 20))
     .add("criteriaDiskInstance", queueCriteria.archiveFile.diskInstance)
     .add("criteriaFileSize", queueCriteria.archiveFile.fileSize)
     .add("reconciliationTime", queueCriteria.archiveFile.reconciliationTime)
     .add("storageClass", queueCriteria.archiveFile.storageClass);
  for (auto & tf:queueCriteria.archiveFile.tapeFiles) {
    std::stringstream tc;
    tc << "tapeCopy" << tf.first;
    spc.add(tc.str(), tf.second);
  }
  spc.add("selectedVid", selectedVid)
     .add("policyName", queueCriteria.mountPolicy.name)
     .add("policyMaxDrives", queueCriteria.mountPolicy.maxDrivesAllowed)
     .add("policyMinAge", queueCriteria.mountPolicy.retrieveMinRequestAge)
     .add("policyPriority", queueCriteria.mountPolicy.retrievePriority)
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "Queued retrieve request");
}

//------------------------------------------------------------------------------
// deleteArchive
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile Scheduler::deleteArchive(const std::string &instanceName, const common::dataStructures::DeleteArchiveRequest &request) {
  // We have different possible scenarios here. The file can be safe in the catalogue,
  // fully queued, or partially queued.
  // First, make sure the file is not queued anymore.
  try {
    m_db.deleteArchiveRequest(instanceName, request.archiveFileID);
  } catch (exception::Exception &dbEx) {
    // The file was apparently not queued. If we fail to remove it from the catalogue, then it is an error.
    return m_catalogue.deleteArchiveFile(instanceName, request.archiveFileID);
  }
  // We did delete the file from the queue. It hence might be absent from the catalogue.
  // Errors are not fatal here (so we filter them out).
  try {
    return m_catalogue.deleteArchiveFile(instanceName, request.archiveFileID);
  } catch (exception::UserError &) {}
  return common::dataStructures::ArchiveFile();
}

//------------------------------------------------------------------------------
// cancelRetrieve
//------------------------------------------------------------------------------
void Scheduler::cancelRetrieve(const std::string &instanceName, const common::dataStructures::CancelRetrieveRequest &request) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// updateFileInfo
//------------------------------------------------------------------------------
void Scheduler::updateFileInfo(const std::string &instanceName, const common::dataStructures::UpdateFileInfoRequest &request) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// updateFileStorageClass
//------------------------------------------------------------------------------
void Scheduler::updateFileStorageClass(const std::string &instanceName, const common::dataStructures::UpdateFileStorageClassRequest &request) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// listStorageClass
//------------------------------------------------------------------------------
std::list<common::dataStructures::StorageClass> Scheduler::listStorageClass(const std::string &instanceName, const common::dataStructures::ListStorageClassRequest &request) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// labelTape
//------------------------------------------------------------------------------
void Scheduler::queueLabel(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force, const bool lbp, const optional<std::string> &tag) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// repack
//------------------------------------------------------------------------------
void Scheduler::queueRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const optional<std::string> &tag, const common::dataStructures::RepackType) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// cancelRepack
//------------------------------------------------------------------------------
void Scheduler::cancelRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getRepacks
//------------------------------------------------------------------------------
std::list<common::dataStructures::RepackInfo> Scheduler::getRepacks(const common::dataStructures::SecurityIdentity &cliIdentity) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getRepack
//------------------------------------------------------------------------------
common::dataStructures::RepackInfo Scheduler::getRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__); 
}

//------------------------------------------------------------------------------
// shrink
//------------------------------------------------------------------------------
void Scheduler::shrink(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapepool) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
void Scheduler::queueVerify(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const optional<std::string> &tag, const optional<uint64_t> numberOfFiles) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// cancelVerify
//------------------------------------------------------------------------------
void Scheduler::cancelVerify(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getVerifys
//------------------------------------------------------------------------------
std::list<common::dataStructures::VerifyInfo> Scheduler::getVerifys(const common::dataStructures::SecurityIdentity &cliIdentity) const {
  return std::list<common::dataStructures::VerifyInfo>(); 
}

//------------------------------------------------------------------------------
// getVerify
//------------------------------------------------------------------------------
common::dataStructures::VerifyInfo Scheduler::getVerify(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// readTest
//------------------------------------------------------------------------------
common::dataStructures::ReadTestResult Scheduler::readTest(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t firstFSeq, const uint64_t lastFSeq, const bool checkChecksum, const std::string &output, const std::string &tag) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// writeTest
//------------------------------------------------------------------------------
common::dataStructures::WriteTestResult Scheduler::writeTest(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const std::string &inputFile, const std::string &tag) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// write_autoTest
//------------------------------------------------------------------------------
common::dataStructures::WriteTestResult Scheduler::write_autoTest(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid,
        const uint64_t numberOfFiles, const uint64_t fileSize, const common::dataStructures::TestSourceType testSourceType, const std::string &tag) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getDesiredDriveState
//------------------------------------------------------------------------------
common::dataStructures::DesiredDriveState Scheduler::getDesiredDriveState(const std::string& driveName) {
  auto driveStates = m_db.getDriveStates();
  for (auto & d: driveStates) {
    if (d.driveName == driveName) {
      return d.desiredDriveState;
    }
  }
  throw NoSuchDrive ("In Scheduler::getDesiredDriveState(): no such drive");
}

//------------------------------------------------------------------------------
// setDesiredDriveState
//------------------------------------------------------------------------------
void Scheduler::setDesiredDriveState(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) {
  common::dataStructures::DesiredDriveState desiredDriveState;
  desiredDriveState.up = up;
  desiredDriveState.forceDown = force;
  m_db.setDesiredDriveState(driveName, desiredDriveState);
}

//------------------------------------------------------------------------------
// setDesiredDriveState
//------------------------------------------------------------------------------
void Scheduler::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo, common::dataStructures::MountType type, common::dataStructures::DriveStatus status) {
  // TODO: mount type should be transmitted too.
  m_db.reportDriveStatus(driveInfo, type, status, time(NULL));
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::ArchiveJob> > Scheduler::getPendingArchiveJobs() const {
  return m_db.getArchiveJobs();
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveJob> Scheduler::getPendingArchiveJobs(const std::string &tapePoolName) const {
  if(!m_catalogue.tapePoolExists(tapePoolName)) {
    throw exception::Exception(std::string("Tape pool ") + tapePoolName + " does not exist");
  }
  return m_db.getArchiveJobs(tapePoolName);
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::RetrieveJob> > Scheduler::getPendingRetrieveJobs() const {
  return m_db.getRetrieveJobs();
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::list<common::dataStructures::RetrieveJob> Scheduler::getPendingRetrieveJobs(const std::string& vid) const {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

//------------------------------------------------------------------------------
// getDriveStates
//------------------------------------------------------------------------------
std::list<common::dataStructures::DriveState> Scheduler::getDriveStates(const common::dataStructures::SecurityIdentity &cliIdentity) const {
  return m_db.getDriveStates();
}

//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<TapeMount> Scheduler::getNextMount(const std::string &logicalLibraryName, const std::string &driveName) {
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
  
  // The library information is not know for the tapes involved in retrieves. We 
  // need to query the catalogue now about all those tapes.
  // Build the list of tapes.
  std::set<std::string> tapeSet;
  for (auto &m:mountInfo->potentialMounts) {
    if (m.type==common::dataStructures::MountType::Retrieve) tapeSet.insert(m.vid);
  }
  if (tapeSet.size()) {
    auto tapesInfo=m_catalogue.getTapesByVid(tapeSet);
    for (auto &m:mountInfo->potentialMounts) {
      if (m.type==common::dataStructures::MountType::Retrieve) {
        m.logicalLibrary=tapesInfo[m.vid].logicalLibraryName;
        m.tapePool=tapesInfo[m.vid].tapePoolName;
      }
    }
  }
  
  // We should now filter the potential mounts to keep only the ones we are
  // compatible with (match the logical library for retrieves).
  // We also only want the potential mounts for which we still have 
  // We cannot filter the archives yet
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    if (m->type == common::dataStructures::MountType::Retrieve && m->logicalLibrary != logicalLibraryName) {
      m = mountInfo->potentialMounts.erase(m);
    } else {
      m++;
    }
  }
  
  // With the existing mount list, we can now populate the potential mount list
  // with the per tape pool existing mount statistics.
  typedef std::pair<std::string, common::dataStructures::MountType> tpType;
  std::map<tpType, uint32_t> existingMountsSummary;
  for (auto & em: mountInfo->existingMounts) {
    // If a mount is still listed for our own drive, it is a leftover that we disregard.
    if (em.driveName!=driveName) {
      try {
        existingMountsSummary.at(tpType(em.tapePool, em.type))++;
      } catch (std::out_of_range &) {
        existingMountsSummary[tpType(em.tapePool, em.type)] = 1;
      }
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
  std::list<catalogue::TapeForWriting> tapeList;
  if (std::count_if(
        mountInfo->potentialMounts.cbegin(), mountInfo->potentialMounts.cend(), 
        [](decltype(*mountInfo->potentialMounts.cbegin())& m){ return m.type == common::dataStructures::MountType::Archive; } )) {
    tapeList = m_catalogue.getTapesForWriting(logicalLibraryName);
  }
       
  auto fullTapeList=m_catalogue.getTapes(catalogue::TapeSearchCriteria());
  for (auto & ftle: fullTapeList) {
    ftle.capacityInBytes++;
  }
        
  for (auto & t:tapeList) {
    auto const & tc=t;
    uint64_t cap = tc.capacityInBytes+1;
    cap++;
  }  
  // We can now simply iterate on the candidates until we manage to create a
  // mount for one of them
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (m->type==common::dataStructures::MountType::Archive) {
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
                utils::getShortHostname(), 
                time(NULL)).release());
            internalRet->m_sessionRunning = true;
            internalRet->setDriveStatus(common::dataStructures::DriveStatus::Starting);
            return std::unique_ptr<TapeMount> (internalRet.release());
          } catch (exception::Exception & ex) {
            continue;
          }
        }
      }
    } else if (m->type==common::dataStructures::MountType::Retrieve) {
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
              utils::getShortHostname(), 
              time(NULL))));
          internalRet->m_sessionRunning = true;
          internalRet->m_diskRunning = true;
          internalRet->m_tapeRunning = true;
          internalRet->setDriveStatus(common::dataStructures::DriveStatus::Starting);
          return std::unique_ptr<TapeMount> (internalRet.release()); 
        } catch (exception::Exception & ex) {
          std::string debug=ex.getMessageValue();
          continue;
        }
      }
    } else {
      throw std::runtime_error("In Scheduler::getNextMount unexpected mount type");
    }
  }
  return std::unique_ptr<TapeMount>();
}

} // namespace cta
