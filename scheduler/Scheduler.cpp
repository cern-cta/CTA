/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "ArchiveMount.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"
#include "common/exception/NonRetryableError.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "disk/RadosStriperPool.hpp"
#include "DiskReportRunner.hpp"
#include "DriveConfig.hpp"
#include "objectstore/RepackRequest.hpp"
#include "OStoreDB/OStoreDB.hpp"
#include "RetrieveMount.hpp"
#include "RetrieveRequestDump.hpp"
#include "Scheduler.hpp"
#include "TapeDrivesCatalogueState.hpp"

#include <iostream>
#include <sstream>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <random>
#include <chrono>
#include <cstdlib>

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Scheduler::Scheduler(
  catalogue::Catalogue &catalogue,
  SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount):
    m_catalogue(catalogue), m_db(db), m_minFilesToWarrantAMount(minFilesToWarrantAMount),
    m_minBytesToWarrantAMount(minBytesToWarrantAMount) {
      m_tapeDrivesState = cta::make_unique<TapeDrivesCatalogueState>(m_catalogue);
    }

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Scheduler::~Scheduler() throw() { }

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void Scheduler::ping(log::LogContext & lc) {
  cta::utils::Timer t;
  m_catalogue.ping();
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  m_db.ping();
  auto schedulerDbTime = t.secs(cta::utils::Timer::resetCounter);
  checkNeededEnvironmentVariables();
  auto checkEnvironmentVariablesTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime)
     .add("checkEnvironmentVariablesTime",checkEnvironmentVariablesTime);
  lc.log(log::INFO, "In Scheduler::ping(): success.");
}

//------------------------------------------------------------------------------
// waitSchedulerDbThreads
//------------------------------------------------------------------------------
void Scheduler::waitSchedulerDbSubthreadsComplete() {
  m_db.waitSubthreadsComplete();
}

//------------------------------------------------------------------------------
// authorizeAdmin
//------------------------------------------------------------------------------
void Scheduler::authorizeAdmin(const common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc){
  cta::utils::Timer t;
  if(!(m_catalogue.isAdmin(cliIdentity))) {
    std::stringstream msg;
    msg << "User: " << cliIdentity.username << " on host: " << cliIdentity.host << " is not authorized to execute CTA admin commands";
    throw exception::UserError(msg.str());
  }
  auto catalogueTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueTime", catalogueTime);
  lc.log(log::INFO, "In Scheduler::authorizeAdmin(): success.");
}

//------------------------------------------------------------------------------
// checkAndGetNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t Scheduler::checkAndGetNextArchiveFileId(const std::string &instanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user, log::LogContext &lc) {
  cta::utils::Timer t;
  const uint64_t archiveFileId = m_catalogue.checkAndGetNextArchiveFileId(instanceName, storageClassName, user);
  const auto catalogueTime = t.secs();
  const auto schedulerDbTime = catalogueTime;

  log::ScopedParamContainer spc(lc);
  spc.add("instanceName", instanceName)
     .add("username", user.name)
     .add("usergroup", user.group)
     .add("storageClass", storageClassName)
     .add("fileId", archiveFileId)
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "Checked request and got next archive file ID");

  return archiveFileId;
}

//------------------------------------------------------------------------------
// queueArchiveWithGivenId
//------------------------------------------------------------------------------
std::string Scheduler::queueArchiveWithGivenId(const uint64_t archiveFileId, const std::string &instanceName,
  const cta::common::dataStructures::ArchiveRequest &request, log::LogContext &lc) {
  cta::utils::Timer t;
  using utils::postEllipsis;
  using utils::midEllipsis;

  if (!request.fileSize)
    throw cta::exception::UserError(std::string("Rejecting archive request for zero-length file: ")+request.diskFileInfo.path);

  const auto queueCriteria = m_catalogue.getArchiveFileQueueCriteria(instanceName, request.storageClass,
    request.requester);
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);

  const common::dataStructures::ArchiveFileQueueCriteriaAndFileId catalogueInfo(archiveFileId,
    queueCriteria.copyToPoolMap, queueCriteria.mountPolicy);

  std::string archiveReqAddr = m_db.queueArchive(instanceName, request, catalogueInfo, lc);
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
     .add("diskFilePath", request.diskFileInfo.path)
     .add("diskFileOwnerUid", request.diskFileInfo.owner_uid)
     .add("diskFileGid", request.diskFileInfo.gid)
     .add("archiveReportURL", midEllipsis(request.archiveReportURL, 50, 15))
     .add("archiveErrorReportURL", midEllipsis(request.archiveErrorReportURL, 50, 15))
     .add("creationHost", request.creationLog.host)
     .add("creationTime", request.creationLog.time)
     .add("creationUser", request.creationLog.username)
     .add("requesterName", request.requester.name)
     .add("requesterGroup", request.requester.group)
     .add("srcURL", midEllipsis(request.srcURL, 50, 15))
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
  request.checksumBlob.addFirstChecksumToLog(spc);
  lc.log(log::INFO, "Queued archive request");
  return archiveReqAddr;
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
std::string Scheduler::queueRetrieve(
  const std::string &instanceName,
  common::dataStructures::RetrieveRequest &request,
  log::LogContext & lc) {
  using utils::postEllipsis;
  using utils::midEllipsis;
  utils::Timer t;
  // Get the queue criteria
  common::dataStructures::RetrieveFileQueueCriteria queueCriteria;
  queueCriteria = m_catalogue.prepareToRetrieveFile(instanceName, request.archiveFileID, request.requester, request.activity, lc);
  queueCriteria.archiveFile.diskFileInfo = request.diskFileInfo;

  // The following block of code is a temporary fix for the following CTA issue:
  //
  //   cta/CTA#777 Minimize mounts for dual copy tape pool recalls
  //
  // The code tries to force a recall to use the tape copy with the lowest
  // tape copy number.  On the one hand this increases wear and tear on tapes
  // containing files with lower tape copy numbers but on the other hand it
  // reduces the number of overall number tape mounts in situations where files
  // with multiple tape copies are being recalled.
  {
    const auto lowestCopyNbTapeFileIter = std::min_element(
      queueCriteria.archiveFile.tapeFiles.begin(), queueCriteria.archiveFile.tapeFiles.end(),
      [](const common::dataStructures::TapeFile &a, const common::dataStructures::TapeFile &b) -> bool
        {return a.copyNb < b.copyNb;});

    if (queueCriteria.archiveFile.tapeFiles.end() != lowestCopyNbTapeFileIter) {
      const common::dataStructures::TapeFile lowestCopyNbTapeFile = *lowestCopyNbTapeFileIter;
      queueCriteria.archiveFile.tapeFiles.clear();
      queueCriteria.archiveFile.tapeFiles.push_back(lowestCopyNbTapeFile);
    }
  }

  auto diskSystemList = m_catalogue.getAllDiskSystems();
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  // By default, the scheduler makes its decision based on all available vids. But if a vid is specified in the protobuf,
  // ignore all the others.
  if(request.vid) {
    queueCriteria.archiveFile.tapeFiles.removeAllVidsExcept(*request.vid);
    if(queueCriteria.archiveFile.tapeFiles.empty()) {
      exception::UserError ex;
      ex.getMessage() << "VID " << *request.vid << " does not contain a tape copy of file with archive file ID " << request.archiveFileID;
      throw ex;
    }
  }

  // Determine disk system for this request, if any
  optional<std::string> diskSystemName;
  try {
    diskSystemName = diskSystemList.getDSName(request.dstURL);
  } catch (std::out_of_range&) {}
  auto requestInfo = m_db.queueRetrieve(request, queueCriteria, diskSystemName, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("fileId", request.archiveFileID)
     .add("instanceName", instanceName)
     .add("diskFilePath", request.diskFileInfo.path)
     .add("diskFileOwnerUid", request.diskFileInfo.owner_uid)
     .add("diskFileGid", request.diskFileInfo.gid)
     .add("dstURL", request.dstURL)
     .add("errorReportURL", request.errorReportURL)
     .add("creationHost", request.creationLog.host)
     .add("creationTime", request.creationLog.time)
     .add("creationUser", request.creationLog.username)
     .add("requesterName", request.requester.name)
     .add("requesterGroup", request.requester.group)
     .add("criteriaArchiveFileId", queueCriteria.archiveFile.archiveFileID)
     .add("criteriaCreationTime", queueCriteria.archiveFile.creationTime)
     .add("criteriaDiskFileId", queueCriteria.archiveFile.diskFileId)
     .add("criteriaDiskFileOwnerUid", queueCriteria.archiveFile.diskFileInfo.owner_uid)
     .add("criteriaDiskInstance", queueCriteria.archiveFile.diskInstance)
     .add("criteriaFileSize", queueCriteria.archiveFile.fileSize)
     .add("reconciliationTime", queueCriteria.archiveFile.reconciliationTime)
     .add("storageClass", queueCriteria.archiveFile.storageClass);
  queueCriteria.archiveFile.checksumBlob.addFirstChecksumToLog(spc);
  uint32_t copyNumber=0;
  for (auto & tf:queueCriteria.archiveFile.tapeFiles) {
    std::stringstream tc;
    tc << "tapeTapefile" << copyNumber++;
    spc.add(tc.str(), tf);
  }
  spc.add("selectedVid", requestInfo.selectedVid)
     .add("verifyOnly", request.isVerifyOnly)
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime)
     .add("policyName", queueCriteria.mountPolicy.name)
     .add("policyMinAge", queueCriteria.mountPolicy.retrieveMinRequestAge)
     .add("policyPriority", queueCriteria.mountPolicy.retrievePriority)
     .add("retrieveRequestId", requestInfo.requestId);
  if (request.activity)
    spc.add("activity", request.activity.value());
  lc.log(log::INFO, "Queued retrieve request");
  return requestInfo.requestId;
}

//------------------------------------------------------------------------------
// deleteArchive
//------------------------------------------------------------------------------
void Scheduler::deleteArchive(const std::string &instanceName, const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) {
  // We have different possible scenarios here. The file can be safe in the catalogue,
  // fully queued, or partially queued.
  // First, make sure the file is not queued anymore.
  utils::Timer t;
  log::TimingList tl;
  if(request.address) {
    //Check if address is provided, we can remove the request from the objectstore
    m_db.cancelArchive(request,lc);
  }
  tl.insertAndReset("schedulerDbTime",t);
  m_catalogue.moveArchiveFileToRecycleLog(request,lc);
  tl.insertAndReset("catalogueTime",t);
  log::ScopedParamContainer spc(lc);
  tl.addToLog(spc);
  lc.log(log::INFO, "In Scheduler::deleteArchive(): success.");
}

//------------------------------------------------------------------------------
// abortRetrieve
//------------------------------------------------------------------------------
void Scheduler::abortRetrieve(const std::string &instanceName, const common::dataStructures::CancelRetrieveRequest &request, log::LogContext & lc) {
  m_db.cancelRetrieve(instanceName, request, lc);
}

void Scheduler::deleteFailed(const std::string &objectId, log::LogContext & lc) {
  m_db.deleteFailed(objectId, lc);
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
void Scheduler::queueLabel(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

void Scheduler::checkTapeCanBeRepacked(const std::string & vid, const SchedulerDatabase::QueueRepackRequest & repackRequest){
  std::set<std::string> vidToRepack({vid});
  try{
    auto vidToTapesMap = m_catalogue.getTapesByVid(vidToRepack); //throws an exception if the vid is not found on the database
    cta::common::dataStructures::Tape tapeToCheck = vidToTapesMap.at(vid);
    if(!tapeToCheck.full){
      throw exception::UserError("You must set the tape as full before repacking it.");
    }
    if(tapeToCheck.state == common::dataStructures::Tape::BROKEN){
      throw exception::UserError(std::string("You cannot repack a tape that is ") + common::dataStructures::Tape::stateToString(common::dataStructures::Tape::BROKEN) + ".");
    }
    if(tapeToCheck.isDisabled() && !repackRequest.m_forceDisabledTape){
      throw exception::UserError(std::string("You cannot repack a ") + common::dataStructures::Tape::stateToString(common::dataStructures::Tape::DISABLED)+ " tape. You can force it by using the flag --disabledtape.");
    }
  } catch(const exception::UserError& userEx){
    throw userEx;
  } catch(const cta::exception::Exception & ex){
    throw exception::UserError("The VID provided for repacking does not exist");
  }
}

//------------------------------------------------------------------------------
// repack
//------------------------------------------------------------------------------
void Scheduler::queueRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext & lc) {
  // Check request sanity
  SchedulerDatabase::QueueRepackRequest repackRequestToQueue = repackRequest;
  repackRequestToQueue.m_creationLog = common::dataStructures::EntryLog(cliIdentity.username,cliIdentity.host,::time(nullptr));
  std::string vid = repackRequest.m_vid;
  std::string repackBufferURL = repackRequest.m_repackBufferURL;
  if (vid.empty()) throw exception::UserError("Empty VID name.");
  if (repackBufferURL.empty()) throw exception::UserError("Empty buffer URL.");
  utils::Timer t;
  checkTapeCanBeRepacked(vid,repackRequestToQueue);
  std::string repackRequestAddress = m_db.queueRepack(repackRequestToQueue, lc);
  log::TimingList tl;
  tl.insertAndReset("schedulerDbTime", t);
  log::ScopedParamContainer params(lc);
  params.add("tapeVid", vid)
        .add("repackType", toString(repackRequest.m_repackType))
        .add("forceDisabledTape", repackRequest.m_forceDisabledTape)
        .add("mountPolicy", repackRequest.m_mountPolicy.name)
        .add("noRecall", repackRequest.m_noRecall)
        .add("creationHostName",repackRequestToQueue.m_creationLog.host)
        .add("creationUserName",repackRequestToQueue.m_creationLog.username)
        .add("creationTime",repackRequestToQueue.m_creationLog.time)
        .add("bufferURL", repackRequest.m_repackBufferURL)
        .add("repackRequestAddress", repackRequestAddress);
  tl.addToLog(params);
  lc.log(log::INFO, "In Scheduler::queueRepack(): success.");
}

//------------------------------------------------------------------------------
// cancelRepack
//------------------------------------------------------------------------------
void Scheduler::cancelRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, log::LogContext & lc) {
  m_db.cancelRepack(vid, lc);
}

//------------------------------------------------------------------------------
// getRepacks
//------------------------------------------------------------------------------
std::list<common::dataStructures::RepackInfo> Scheduler::getRepacks() {
  return m_db.getRepackInfo();
}

//------------------------------------------------------------------------------
// getRepack
//------------------------------------------------------------------------------
common::dataStructures::RepackInfo Scheduler::getRepack(const std::string &vid) {
  return m_db.getRepackInfo(vid);
}

//------------------------------------------------------------------------------
// isBeingRepacked
//------------------------------------------------------------------------------
bool Scheduler::isBeingRepacked(const std::string &vid) {
  try {
    getRepack(vid);
    return true;
  } catch(cta::exception::UserError) {
    return false;
  }
}

//------------------------------------------------------------------------------
// promoteRepackRequestsToToExpand
//------------------------------------------------------------------------------
void Scheduler::promoteRepackRequestsToToExpand(log::LogContext & lc) {
  // We target 2 fresh requests available for processing (ToExpand or Starting).
  const size_t targetAvailableRequests = 2;
  // Dry-run test to check if promotion is needed.
  auto repackStatsNL = m_db.getRepackStatisticsNoLock();
  // Statistics are supposed to be initialized for each status value. We only try to
  // expand if there are requests available in Pending status.
  typedef common::dataStructures::RepackInfo::Status Status;
  if (repackStatsNL->at(Status::Pending) &&
          (targetAvailableRequests > repackStatsNL->at(Status::ToExpand) + repackStatsNL->at(Status::Starting))) {
    // Let's try to promote a repack request. Take the lock.
    repackStatsNL.reset();
    decltype(m_db.getRepackStatistics()) repackStats;
    try {
      repackStats = m_db.getRepackStatistics();
    } catch (SchedulerDatabase::RepackRequestStatistics::NoPendingRequestQueue &) {
      // Nothing to promote after all.
      return;
    }
    if (repackStats->at(Status::Pending) &&
            (targetAvailableRequests > repackStats->at(Status::ToExpand) + repackStats->at(Status::Starting))) {
      auto requestsToPromote = targetAvailableRequests;
      requestsToPromote -= repackStats->at(Status::ToExpand);
      requestsToPromote -= repackStats->at(Status::Starting);
      auto stats = repackStats->promotePendingRequestsForExpansion(requestsToPromote, lc);
      log::ScopedParamContainer params(lc);
      params.add("promotedRequests", stats.promotedRequests)
            .add("pendingBefore", stats.pendingBefore)
            .add("toEnpandBefore", stats.toEnpandBefore)
            .add("pendingAfter", stats.pendingAfter)
            .add("toExpandAfter", stats.toExpandAfter);
      lc.log(log::INFO,"In Scheduler::promoteRepackRequestsToToExpand(): Promoted repack request to \"to expand\"");
    }
  }
}

//------------------------------------------------------------------------------
// getNextRepackRequestToExpand
//------------------------------------------------------------------------------
std::unique_ptr<RepackRequest> Scheduler::getNextRepackRequestToExpand() {
  std::unique_ptr<cta::SchedulerDatabase::RepackRequest> repackRequest;
  repackRequest = m_db.getNextRepackJobToExpand();
  if(repackRequest != nullptr){
    std::unique_ptr<RepackRequest> ret(new RepackRequest());
    ret->m_dbReq.reset(repackRequest.release());
    return std::move(ret);
  }
  return nullptr;
}

//------------------------------------------------------------------------------
// expandRepackRequest
//------------------------------------------------------------------------------
void Scheduler::expandRepackRequest(std::unique_ptr<RepackRequest>& repackRequest, log::TimingList& timingList, utils::Timer& t, log::LogContext& lc) {
  auto repackInfo = repackRequest->getRepackInfo();

  typedef cta::common::dataStructures::RepackInfo::Type RepackType;

  //We need to get the ArchiveRoutes to allow the retrieval of the tapePool in which the
  //tape where the file is is located
  std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue.getArchiveRoutes();
  timingList.insertAndReset("catalogueGetArchiveRoutesTime",t);
  //To identify the routes, we need to have both the dist instance name and the storage class name
  //thus, the key of the map is a pair of string
  cta::common::dataStructures::ArchiveRoute::FullMap archiveRoutesMap;
  for(auto route: routes){
    //insert the route into the map to allow a quick retrieval
    archiveRoutesMap[route.storageClassName][route.copyNb] = route;
  }
  uint64_t fSeq;
  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles totalStatsFile;
  repackRequest->m_dbReq->fillLastExpandedFSeqAndTotalStatsFile(fSeq,totalStatsFile);
  timingList.insertAndReset("fillTotalStatsFileBeforeExpandTime",t);
  cta::catalogue::Catalogue::ArchiveFileItor archiveFilesForCatalogue = m_catalogue.getArchiveFilesForRepackItor(repackInfo.vid, fSeq);
  timingList.insertAndReset("catalogueGetArchiveFilesForRepackItorTime",t);

  std::stringstream dirBufferURL;
  dirBufferURL << repackInfo.repackBufferBaseURL << "/" << repackInfo.vid << "/";
  std::set<std::string> filesInDirectory;
  std::unique_ptr<cta::disk::Directory> dir;
  if(archiveFilesForCatalogue.hasMore()){
    //We only create the folder if there are some files to Repack
    cta::disk::DirectoryFactory dirFactory;
    dir.reset(dirFactory.createDirectory(dirBufferURL.str()));
    try {
      if(dir->exist()){
        //Repack tape repair workflow
        filesInDirectory = dir->getFilesName();
      } else {
        if(repackInfo.noRecall){
          //The buffer directory should be created if the --no-recall flag has been passed
          //So we throw an exception
          throw ExpandRepackRequestException("In Scheduler::expandRepackRequest(): the flag --no-recall is set but no buffer directory has been created.");
        }
        dir->mkdir();
      }
    } catch (const cta::exception::XrootCl &ex) {
      throw ExpandRepackRequestException("In Scheduler::expandRepackRequest(): errors while doing some checks on the repack buffer. ExceptionMsg = " + ex.getMessageValue());
    }
  }

  std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue.getStorageClasses();

  repackRequest->m_dbReq->setExpandStartedAndChangeStatus();
  uint64_t nbRetrieveSubrequestsQueued = 0;

  std::list<cta::common::dataStructures::ArchiveFile> archiveFilesFromCatalogue;

  while(archiveFilesForCatalogue.hasMore()){
    archiveFilesFromCatalogue.push_back(archiveFilesForCatalogue.next());
  }

  if(repackInfo.noRecall){
    archiveFilesFromCatalogue.remove_if([&repackInfo, &filesInDirectory](const common::dataStructures::ArchiveFile & archiveFile){
      //We remove all the elements that are not in the repack buffer so that we don't recall them
      return std::find_if(filesInDirectory.begin(), filesInDirectory.end(),[&archiveFile, &repackInfo](const std::string & fseq){
        //If we find a tape file that has the current fseq and belongs to the VID to repack, then we DON'T remove it from
        //the archiveFilesFromCatalogue list
        return std::find_if(archiveFile.tapeFiles.begin(), archiveFile.tapeFiles.end(),[&repackInfo, &fseq](const common::dataStructures::TapeFile & tapeFile){
          //Can we find, in the archiveFilesFromCatalogue list an archiveFile that contains a tapefile that belongs to the VID to repack and that has the
          //fseq of the current file read from the filesInDirectory list ?
          return tapeFile.vid == repackInfo.vid && tapeFile.fSeq == cta::utils::toUint64(cta::utils::removePrefix(fseq,'0'));
        }) != archiveFile.tapeFiles.end();
      }) == filesInDirectory.end();
    });
  }

  std::list<SchedulerDatabase::RepackRequest::Subrequest> retrieveSubrequests;
  uint64_t maxAddedFSeq = 0;
  while(!archiveFilesFromCatalogue.empty()) {
    fSeq++;
    retrieveSubrequests.push_back(cta::SchedulerDatabase::RepackRequest::Subrequest());
    auto archiveFile = archiveFilesFromCatalogue.front();
    archiveFilesFromCatalogue.pop_front();
    auto & retrieveSubRequest  = retrieveSubrequests.back();

    retrieveSubRequest.archiveFile = archiveFile;
    retrieveSubRequest.fSeq = std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max();

     //Check that all the archive routes have been configured, if one archive route is missing, we fail the repack request.
    auto archiveFileRoutes = archiveRoutesMap[archiveFile.storageClass];
    auto storageClassOfArchiveFile = std::find_if(storageClasses.begin(),storageClasses.end(),[&archiveFile](const common::dataStructures::StorageClass& sc){
      return sc.name == archiveFile.storageClass;
    });

    if(storageClassOfArchiveFile == storageClasses.end()) {
      //No storage class have been found for the current tapefile throw an exception
      deleteRepackBuffer(std::move(dir),lc);
      std::ostringstream oss;
      oss << "In Scheduler::expandRepackRequest(): No storage class have been found for the file to repack. ArchiveFileID=" << archiveFile.archiveFileID << " StorageClass of the file=" << archiveFile.storageClass;
      throw ExpandRepackRequestException(oss.str());
    }

    common::dataStructures::StorageClass sc = *storageClassOfArchiveFile;

    // We have to determine which copynbs we want to rearchive, and under which fSeq we record this file.
    if (repackInfo.type == RepackType::MoveAndAddCopies || repackInfo.type == RepackType::MoveOnly) {
      // determine which fSeq(s) (normally only one) lives on this tape.
      for (auto & tc: archiveFile.tapeFiles) if (tc.vid == repackInfo.vid) {
        // We make the (reasonable) assumption that the archive file only has one copy on this tape.
        // If not, we will ensure the subrequest is filed under the lowest fSeq existing on this tape.
        // This will prevent double subrequest creation (we already have such a mechanism in case of crash and
        // restart of expansion.

        //Here, test that the archive route of the copyNb of the tape file is configured
        try {
          archiveFileRoutes.at(tc.copyNb);
        } catch (const std::out_of_range & ex) {
          deleteRepackBuffer(std::move(dir),lc);
          std::ostringstream oss;
          oss << "In Scheduler::expandRepackRequest(): the file archiveFileID=" << archiveFile.archiveFileID << ", copyNb=" << std::to_string(tc.copyNb) << ", storageClass=" << archiveFile.storageClass << " does not have any archive route for archival.";
          throw ExpandRepackRequestException(oss.str());
        }

        totalStatsFile.totalFilesToArchive += 1;
        totalStatsFile.totalBytesToArchive += retrieveSubRequest.archiveFile.fileSize;
        retrieveSubRequest.copyNbsToRearchive.insert(tc.copyNb);
        retrieveSubRequest.fSeq = tc.fSeq;
      }
    }

    if(repackInfo.type == RepackType::AddCopiesOnly || repackInfo.type == RepackType::MoveAndAddCopies){
      //We are in the case where we possibly need to create new copies (if the number of copies the storage class of the current ArchiveFile
      //is greater than the number of tape files we have in the current ArchiveFile)
      uint64_t nbFilesAlreadyArchived = getNbFilesAlreadyArchived(archiveFile);
      uint64_t nbCopiesInStorageClass = sc.nbCopies;
      uint64_t filesToArchive = nbCopiesInStorageClass - nbFilesAlreadyArchived;
      if(filesToArchive > 0){
        totalStatsFile.totalFilesToArchive += filesToArchive;
        totalStatsFile.totalBytesToArchive += (filesToArchive * archiveFile.fileSize);
        std::set<uint64_t> copyNbsAlreadyInCTA;
        for (auto & tc: archiveFile.tapeFiles) {
          copyNbsAlreadyInCTA.insert(tc.copyNb);
          if (tc.vid == repackInfo.vid) {
            // We make the (reasonable) assumption that the archive file only has one copy on this tape.
            // If not, we will ensure the subrequest is filed under the lowest fSeq existing on this tape.
            // This will prevent double subrequest creation (we already have such a mechanism in case of crash and
            // restart of expansion.
            //We found the copy of the file we want to retrieve and archive
            //retrieveSubRequest.fSeq = tc.fSeq;
            if(repackInfo.type == RepackType::AddCopiesOnly)
              retrieveSubRequest.fSeq = (retrieveSubRequest.fSeq == std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max()) ? tc.fSeq : std::max(tc.fSeq, retrieveSubRequest.fSeq);
          }
        }
        for(auto archiveFileRoutesItor = archiveFileRoutes.begin(); archiveFileRoutesItor != archiveFileRoutes.end(); ++archiveFileRoutesItor){
          if(copyNbsAlreadyInCTA.find(archiveFileRoutesItor->first) == copyNbsAlreadyInCTA.end()){
            //We need to archive the missing copy
            retrieveSubRequest.copyNbsToRearchive.insert(archiveFileRoutesItor->first);
          }
        }
        if(retrieveSubRequest.copyNbsToRearchive.size() < filesToArchive){
          deleteRepackBuffer(std::move(dir),lc);
          throw ExpandRepackRequestException("In Scheduler::expandRepackRequest(): Missing archive routes for the creation of the new copies of the files");
        }
      } else {
        if(repackInfo.type == RepackType::AddCopiesOnly){
          //Nothing to Archive so nothing to Retrieve as well
          retrieveSubrequests.pop_back();
          continue;
        }
      }
    }


    std::stringstream fileName;
    fileName << std::setw(9) << std::setfill('0') << retrieveSubRequest.fSeq;
    bool createArchiveSubrequest = false;
    if(filesInDirectory.count(fileName.str())){
      cta::disk::RadosStriperPool radosStriperPool;
      cta::disk::DiskFileFactory fileFactory("",0,radosStriperPool);
      cta::disk::ReadFile *fileReader = fileFactory.createReadFile(dirBufferURL.str() + fileName.str());
      if(fileReader->size() == archiveFile.fileSize){
        createArchiveSubrequest = true;
      }
    }
    if (!createArchiveSubrequest && retrieveSubRequest.fSeq == std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max()) {
      if(!createArchiveSubrequest){
        log::ScopedParamContainer params(lc);
        params.add("fileId", retrieveSubRequest.archiveFile.archiveFileID)
              .add("repackVid", repackInfo.vid);
        lc.log(log::ERR, "In Scheduler::expandRepackRequest(): no fSeq found for this file on this tape.");
        totalStatsFile.totalBytesToRetrieve -= retrieveSubRequest.archiveFile.fileSize;
        totalStatsFile.totalFilesToRetrieve -= 1;
        retrieveSubrequests.pop_back();
      }
    } else {
      if(!createArchiveSubrequest){
        totalStatsFile.totalBytesToRetrieve += retrieveSubRequest.archiveFile.fileSize;
        totalStatsFile.totalFilesToRetrieve += 1;
      } else {
        totalStatsFile.userProvidedFiles += 1;
        retrieveSubRequest.hasUserProvidedFile = true;
      }
      // We found some copies to rearchive. We still have to decide which file path we are going to use.
      // File path will be base URL + /<VID>/<fSeq>
      maxAddedFSeq = std::max(maxAddedFSeq,retrieveSubRequest.fSeq);
      retrieveSubRequest.fileBufferURL = dirBufferURL.str() + fileName.str();
    }
  }
  auto diskSystemList = m_catalogue.getAllDiskSystems();
  timingList.insertAndReset("getDisksystemsListTime",t);
  try{
    // Note: the highest fSeq will be recorded internally in the following call.
    // We know that the fSeq processed on the tape are >= initial fSeq + filesCount - 1 (or fSeq - 1 as we counted).
    // We pass this information to the db for recording in the repack request. This will allow restarting from the right
    // value in case of crash.
    nbRetrieveSubrequestsQueued = repackRequest->m_dbReq->addSubrequestsAndUpdateStats(retrieveSubrequests, archiveRoutesMap, fSeq, maxAddedFSeq, totalStatsFile, diskSystemList, lc);
  } catch(const cta::ExpandRepackRequestException& e){
    deleteRepackBuffer(std::move(dir),lc);
    throw e;
  }
  timingList.insertAndReset("addSubrequestsAndUpdateStatsTime",t);

  log::ScopedParamContainer params(lc);
  params.add("tapeVid",repackInfo.vid);
  timingList.addToLog(params);

  if(archiveFilesFromCatalogue.empty() && totalStatsFile.totalFilesToArchive == 0 && (totalStatsFile.totalFilesToRetrieve == 0 || nbRetrieveSubrequestsQueued == 0)){
    //If no files have been retrieve, the repack buffer will have to be deleted
    //TODO : in case of Repack tape repair, we should not try to delete the buffer
    deleteRepackBuffer(std::move(dir),lc);
  }
  repackRequest->m_dbReq->expandDone();
  lc.log(log::INFO,"In Scheduler::expandRepackRequest(), repack request expanded");
}

//------------------------------------------------------------------------------
// Scheduler::getNextRepackReportBatch
//------------------------------------------------------------------------------
Scheduler::RepackReportBatch Scheduler::getNextRepackReportBatch(log::LogContext& lc) {
  RepackReportBatch ret;
  ret.m_DbBatch = std::move(m_db.getNextRepackReportBatch(lc));
  return ret;
}


//------------------------------------------------------------------------------
// Scheduler::getRepackReportBatches
//------------------------------------------------------------------------------
std::list<Scheduler::RepackReportBatch> Scheduler::getRepackReportBatches(log::LogContext &lc){
  std::list<Scheduler::RepackReportBatch> ret;
  for(auto& reportBatch: m_db.getRepackReportBatches(lc)){
    Scheduler::RepackReportBatch report;
    report.m_DbBatch.reset(reportBatch.release());
    ret.push_back(std::move(report));
  }
  return ret;
}

//------------------------------------------------------------------------------
// Scheduler::getNextSuccessfulRetrieveRepackReportBatch
//------------------------------------------------------------------------------
Scheduler::RepackReportBatch Scheduler::getNextSuccessfulRetrieveRepackReportBatch(log::LogContext &lc){
  Scheduler::RepackReportBatch ret;
  try{
    ret.m_DbBatch.reset(m_db.getNextSuccessfulRetrieveRepackReportBatch(lc).release());
  } catch (OStoreDB::NoRepackReportBatchFound &){
    ret.m_DbBatch = nullptr;
  }
  return ret;
}

//------------------------------------------------------------------------------
// Scheduler::getNextFailedRetrieveRepackReportBatch
//------------------------------------------------------------------------------
Scheduler::RepackReportBatch Scheduler::getNextFailedRetrieveRepackReportBatch(log::LogContext &lc){
  Scheduler::RepackReportBatch ret;
  try{
    ret.m_DbBatch.reset(m_db.getNextFailedRetrieveRepackReportBatch(lc).release());
  } catch (OStoreDB::NoRepackReportBatchFound &){
    ret.m_DbBatch = nullptr;
  }
  return ret;
}

//------------------------------------------------------------------------------
// Scheduler::getNextSuccessfulArchiveRepackReportBatch
//------------------------------------------------------------------------------
Scheduler::RepackReportBatch Scheduler::getNextSuccessfulArchiveRepackReportBatch(log::LogContext &lc){
  Scheduler::RepackReportBatch ret;
  try{
    ret.m_DbBatch.reset(m_db.getNextSuccessfulArchiveRepackReportBatch(lc).release());
  } catch (OStoreDB::NoRepackReportBatchFound &){
    ret.m_DbBatch = nullptr;
  }
  return ret;
}

//------------------------------------------------------------------------------
// Scheduler::getNextFailedArchiveRepackReportBatch
//------------------------------------------------------------------------------
Scheduler::RepackReportBatch Scheduler::getNextFailedArchiveRepackReportBatch(log::LogContext &lc){
  Scheduler::RepackReportBatch ret;
  try{
    ret.m_DbBatch.reset(m_db.getNextFailedArchiveRepackReportBatch(lc).release());
  } catch (OStoreDB::NoRepackReportBatchFound &){
    ret.m_DbBatch = nullptr;
  }
  return ret;
}

//------------------------------------------------------------------------------
// Scheduler::RepackReportBatch::report
//------------------------------------------------------------------------------
void Scheduler::RepackReportBatch::report(log::LogContext& lc) {
  if (nullptr == m_DbBatch) {
    // lc.log(log::DEBUG, "In Scheduler::RepackReportBatch::report(): empty batch.");
  } else {
    m_DbBatch->report(lc);
  }
}

//------------------------------------------------------------------------------
// getDesiredDriveState
//------------------------------------------------------------------------------
common::dataStructures::DesiredDriveState Scheduler::getDesiredDriveState(const std::string& driveName, log::LogContext & lc) {
  utils::Timer t;
  auto driveStates = m_tapeDrivesState->getDriveStates(lc);
  for (auto & driveState : driveStates) {
    if (driveState.driveName == driveName) {
      auto schedulerDbTime = t.secs();
      if (schedulerDbTime > 1) {
        log::ScopedParamContainer spc(lc);
        spc.add("drive", driveName)
           .add("schedulerDbTime", schedulerDbTime);
        lc.log(log::DEBUG, "In Scheduler::getDesiredDriveState(): success.");
      }
      common::dataStructures::DesiredDriveState desiredDriveState;
      desiredDriveState.up = driveState.desiredUp;
      desiredDriveState.forceDown = driveState.desiredForceDown;
      desiredDriveState.reason = driveState.reasonUpDown;
      desiredDriveState.comment = driveState.userComment;
      return desiredDriveState;
    }
  }
  throw NoSuchDrive ("In Scheduler::getDesiredDriveState(): no such drive");
}

//------------------------------------------------------------------------------
// setDesiredDriveState
//------------------------------------------------------------------------------
void Scheduler::setDesiredDriveState(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const common::dataStructures::DesiredDriveState & desiredState, log::LogContext & lc) {
  utils::Timer t;
  m_tapeDrivesState->setDesiredDriveState(driveName, desiredState, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveName)
     .add("up", desiredState.up ? "up" : "down")
     .add("force", desiredState.forceDown ? "yes" : "no")
     .add("reason",desiredState.reason ? desiredState.reason.value() : "")
     .add("comment", desiredState.comment ? desiredState.comment.value() : "")
     .add("schedulerDbTime", schedulerDbTime);
   lc.log(log::INFO, "In Scheduler::setDesiredDriveState(): success.");
}

bool Scheduler::checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo, log::LogContext & lc) {
  try{
    m_tapeDrivesState->checkDriveCanBeCreated(driveInfo);
    return true;
  } catch (cta::TapeDrivesCatalogueState::DriveAlreadyExistsException &ex) {
    log::ScopedParamContainer param(lc);
    param.add("tapeDrive",driveInfo.driveName)
         .add("logicalLibrary",driveInfo.logicalLibrary)
         .add("host",driveInfo.host)
         .add("exceptionMsg",ex.getMessageValue());
    lc.log(log::CRIT,"In Scheduler::checkDriveCanBeCreated(): drive already exists. Reporting fatal error.");
    return false;
  }
}

//------------------------------------------------------------------------------
// removeDrive
//------------------------------------------------------------------------------
void Scheduler::removeDrive(const common::dataStructures::SecurityIdentity &cliIdentity,
  const std::string &driveName, log::LogContext & lc) {
  utils::Timer t;
  m_tapeDrivesState->removeDrive(driveName, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveName)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::removeDrive(): success.");
}

//------------------------------------------------------------------------------
// reportDriveConfig
//------------------------------------------------------------------------------
void Scheduler::reportDriveConfig(const cta::tape::daemon::TpconfigLine& tpConfigLine,const cta::tape::daemon::TapedConfiguration& tapedConfig,log::LogContext& lc) {
  utils::Timer t;
  DriveConfig::setTapedConfiguration(tapedConfig, &m_catalogue, tpConfigLine.unitName);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
   spc.add("drive", tpConfigLine.unitName)
      .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO,"In Scheduler::reportDriveConfig(): success.");
}

//------------------------------------------------------------------------------
// reportDriveStatus
//------------------------------------------------------------------------------
void Scheduler::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  common::dataStructures::MountType type, common::dataStructures::DriveStatus status, log::LogContext & lc) {
  // TODO: mount type should be transmitted too.
  utils::Timer t;
  m_tapeDrivesState->reportDriveStatus(driveInfo, type, status, time(NULL), lc);
  auto schedulerDbTime = t.secs();
  if (schedulerDbTime > 1) {
    log::ScopedParamContainer spc(lc);
    spc.add("drive", driveInfo.driveName)
       .add("schedulerDbTime", schedulerDbTime);
    lc.log(log::DEBUG, "In Scheduler::reportDriveStatus(): success.");
  }
}

void Scheduler::createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
  const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
  const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
  const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc) {
  m_tapeDrivesState->createTapeDriveStatus(driveInfo, desiredState, type, status, tpConfigLine, identity, lc);
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveInfo.driveName);
  lc.log(log::DEBUG, "In Scheduler::createTapeDriveStatus(): success.");
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::ArchiveJob> > Scheduler::getPendingArchiveJobs(log::LogContext & lc) const {
  utils::Timer t;
  auto ret = m_db.getArchiveJobs();
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::getPendingArchiveJobs(): success.");
  return ret;
}

//------------------------------------------------------------------------------
// getPendingArchiveJobs
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveJob> Scheduler::getPendingArchiveJobs(const std::string &tapePoolName, log::LogContext & lc) const {
  utils::Timer t;
  if(!m_catalogue.tapePoolExists(tapePoolName)) {
    throw exception::UserError(std::string("Tape pool ") + tapePoolName + " does not exist");
  }
  auto catalogueTime = t.secs(utils::Timer::resetCounter);
  auto ret = m_db.getArchiveJobs(tapePoolName);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::getPendingArchiveJobs(tapePool): success.");
  return ret;
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::map<std::string, std::list<common::dataStructures::RetrieveJob> > Scheduler::getPendingRetrieveJobs(log::LogContext & lc) const {
  utils::Timer t;
  auto ret =  m_db.getRetrieveJobs();
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::getPendingRetrieveJobs(): success.");
  return ret;
}

//------------------------------------------------------------------------------
// getPendingRetrieveJobs
//------------------------------------------------------------------------------
std::list<common::dataStructures::RetrieveJob> Scheduler::getPendingRetrieveJobs(const std::string& vid, log::LogContext &lc) const {
  utils::Timer t;
  auto ret =  m_db.getRetrieveJobs(vid);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::getPendingRetrieveJobs(): success.");
  return ret;
}

//------------------------------------------------------------------------------
// getDriveStates
//------------------------------------------------------------------------------
std::list<common::dataStructures::TapeDrive> Scheduler::getDriveStates(const common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc) const {
  utils::Timer t;
  auto ret = m_tapeDrivesState->getDriveStates(lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::getDriveStates(): success.");
  return ret;
}

//------------------------------------------------------------------------------
// sortAndGetTapesForMountInfo
//------------------------------------------------------------------------------
void Scheduler::sortAndGetTapesForMountInfo(std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>& mountInfo,
    const std::string & logicalLibraryName, const std::string & driveName, utils::Timer & timer,
    ExistingMountSummaryPerTapepool & existingMountsDistinctTypeSummaryPerTapepool, ExistingMountSummaryPerVo & existingMountsBasicTypeSummaryPerVo, std::set<std::string> & tapesInUse, std::list<catalogue::TapeForWriting> & tapeList,
    double & getTapeInfoTime, double & candidateSortingTime, double & getTapeForWriteTime, log::LogContext & lc) {
  // The library information is not know for the tapes involved in retrieves. We
  // need to query the catalogue now about all those tapes.
  // Build the list of tapes.
  std::set<std::string> retrieveTapeSet;
  for (auto &m:mountInfo->potentialMounts) {
    if (m.type==common::dataStructures::MountType::Retrieve) retrieveTapeSet.insert(m.vid);
  }
  common::dataStructures::VidToTapeMap retrieveTapesInfo;
  if (retrieveTapeSet.size()) {
    retrieveTapesInfo=m_catalogue.getTapesByVid(retrieveTapeSet);
    getTapeInfoTime = timer.secs(utils::Timer::resetCounter);
    for (auto &m:mountInfo->potentialMounts) {
      if (m.type==common::dataStructures::MountType::Retrieve) {
        m.logicalLibrary=retrieveTapesInfo[m.vid].logicalLibraryName;
        m.tapePool=retrieveTapesInfo[m.vid].tapePoolName;
        m.vendor = retrieveTapesInfo[m.vid].vendor;
        m.mediaType = retrieveTapesInfo[m.vid].mediaType;
        m.vo = retrieveTapesInfo[m.vid].vo;
        m.capacityInBytes = retrieveTapesInfo[m.vid].capacityInBytes;
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

  //Get the tapepools of the potential and existing mounts
  std::set<std::string> tapepoolsPotentialOrExistingMounts;
  for (auto & pm: mountInfo->potentialMounts) {
    tapepoolsPotentialOrExistingMounts.insert(pm.tapePool);
  }
  for (auto & em: mountInfo->existingOrNextMounts) {
    tapepoolsPotentialOrExistingMounts.insert(em.tapePool);
  }
  //Get the potential and existing mounts tapepool virtual organization information
  std::map<std::string,common::dataStructures::VirtualOrganization> tapepoolVoMap;
  for (auto & tapepool: tapepoolsPotentialOrExistingMounts) {
    try {
      tapepoolVoMap[tapepool] = m_catalogue.getCachedVirtualOrganizationOfTapepool(tapepool);
    } catch (cta::exception::Exception & ex){
      //The VO of this tapepool does not exist, abort the scheduling as we need it to know the number of allocated drives
      //the VO is allowed to use
      ex.getMessage() << " Aborting scheduling." << std::endl;
      throw ex;
    }
  }

  // With the existing mount list, we can now populate the potential mount list
  // with the per tape pool existing mount statistics.
  for (auto & em: mountInfo->existingOrNextMounts) {
    // If a mount is still listed for our own drive, it is a leftover that we disregard.
    if (em.driveName!=driveName) {
      existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)].totalMounts++;
      existingMountsBasicTypeSummaryPerVo[VirtualOrganizationMountPair(tapepoolVoMap.at(em.tapePool).name,common::dataStructures::getMountBasicType(em.type))].totalMounts++;
      if (em.activity)
        existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)]
          .activityMounts[em.activity.value()].value++;
      if (em.vid.size()) {
        tapesInUse.insert(em.vid);
        log::ScopedParamContainer params(lc);
        params.add("tapeVid", em.vid)
              .add("mountType", common::dataStructures::toString(em.type))
              .add("drive", em.driveName);
        lc.log(log::DEBUG,"In Scheduler::sortAndGetTapesForMountInfo(): tapeAlreadyInUse found.");
      }
    }
  }

  // We can now filter out the potential mounts for which their mount criteria
  // is not yet met, filter out the potential mounts for which the maximum mount
  // quota is already reached, filter out the retrieve requests put to sleep for lack of disk space,
  // and weight the remaining by how much of their quota is reached.
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    // Get summary data
    uint32_t existingMountsDistinctTypesForThisTapepool = 0;
    uint32_t existingMountsBasicTypeForThisVo = 0;
    uint32_t activityMounts = 0;
    common::dataStructures::MountType basicTypeOfThisPotentialMount = common::dataStructures::getMountBasicType(m->type);
    common::dataStructures::VirtualOrganization voOfThisPotentialMount = tapepoolVoMap.at(m->tapePool);
    bool sleepingMount = false;
    try {
      existingMountsDistinctTypesForThisTapepool = existingMountsDistinctTypeSummaryPerTapepool
          .at(TapePoolMountPair(m->tapePool, m->type))
             .totalMounts;
    } catch (std::out_of_range &) {}
    try {
      existingMountsBasicTypeForThisVo = existingMountsBasicTypeSummaryPerVo
          .at(VirtualOrganizationMountPair(voOfThisPotentialMount.name, basicTypeOfThisPotentialMount))
             .totalMounts;
    } catch (std::out_of_range &) {}
    if (m->activityNameAndWeightedMountCount) {
      try {
        activityMounts = existingMountsDistinctTypeSummaryPerTapepool
          .at(TapePoolMountPair(m->tapePool, m->type))
             .activityMounts.at(m->activityNameAndWeightedMountCount.value().activity).value;
      } catch (std::out_of_range &) {}
    }
    uint32_t effectiveExistingMountsForThisTapepool = 0;
    //If we have an archive mount, we don't want
    if (basicTypeOfThisPotentialMount == common::dataStructures::MountType::ArchiveAllTypes) effectiveExistingMountsForThisTapepool = existingMountsDistinctTypesForThisTapepool;
    bool mountPassesACriteria = false;
    uint64_t minBytesToWarrantAMount = m_minBytesToWarrantAMount;
    uint64_t minFilesToWarrantAMount = m_minFilesToWarrantAMount;
    if(m->type == common::dataStructures::MountType::ArchiveForRepack){
      minBytesToWarrantAMount *= 2;
      minFilesToWarrantAMount *= 2;
    }
    if (m->bytesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minBytesToWarrantAMount)
      mountPassesACriteria = true;
    if (m->filesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minFilesToWarrantAMount)
      mountPassesACriteria = true;
    if (!effectiveExistingMountsForThisTapepool && ((time(NULL) - m->oldestJobStartTime) > m->minRequestAge))
      mountPassesACriteria = true;
    if (m->sleepingMount) {
      sleepingMount = true;
    }
    uint64_t maxDrives = 0;
    if(basicTypeOfThisPotentialMount == common::dataStructures::MountType::Retrieve) {
      maxDrives = voOfThisPotentialMount.readMaxDrives;
    } else if (basicTypeOfThisPotentialMount == common::dataStructures::MountType::ArchiveAllTypes) {
      maxDrives = voOfThisPotentialMount.writeMaxDrives;
    }
    if (!mountPassesACriteria || existingMountsBasicTypeForThisVo >= maxDrives || sleepingMount) {
      log::ScopedParamContainer params(lc);
      params.add("tapePool", m->tapePool);
      params.add("vo",voOfThisPotentialMount.name);
      if ( m->type == common::dataStructures::MountType::Retrieve) {
        params.add("tapeVid", m->vid);
      }
      params.add("mountType", common::dataStructures::toString(m->type))
            .add("existingMountsDistinctTypesForThisTapepool", existingMountsDistinctTypesForThisTapepool)
            .add("existingMountsBasicTypeForThisVo",existingMountsBasicTypeForThisVo)
            .add("bytesQueued", m->bytesQueued)
            .add("minBytesToWarrantMount", minBytesToWarrantAMount)
            .add("filesQueued", m->filesQueued)
            .add("minFilesToWarrantMount", minFilesToWarrantAMount)
            .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
            .add("minArchiveRequestAge", m->minRequestAge)
            .add("voReadMaxDrives",voOfThisPotentialMount.readMaxDrives)
            .add("voWriteMaxDrives",voOfThisPotentialMount.writeMaxDrives)
            .add("maxDrives", maxDrives);
      if (sleepingMount) params.add("fullDiskSystem", m->diskSystemSleptFor);
      lc.log(log::DEBUG, "In Scheduler::sortAndGetTapesForMountInfo(): Removing potential mount not passing criteria");
      m = mountInfo->potentialMounts.erase(m);
    } else {
      // For the implementation of this ticket: https://gitlab.cern.ch/cta/CTA/-/issues/948
      // The max drives allowed is not per-tapepool anymore and is not set by the mount policy neither
      // Commenting this line so that we have a trace of what existed previously.
      // m->ratioOfMountQuotaUsed = 1.0L * existingMountsPerTapepool / m->maxDrivesAllowed;
      // Probably this fair-share activities should be per-VO instead of per-tapepool as it is now.
      // If it has to be per-tapepool, then the readMaxDrives and writeMaxDrives should also be implemented in the tapepool
      // populate the mount with a weight
      //m->ratioOfMountQuotaUsed = 1.0L * existingMountsPerTapepool / m->maxDrivesAllowed;
      m->ratioOfMountQuotaUsed = 0.0L;
      if (m->activityNameAndWeightedMountCount) {
        m->activityNameAndWeightedMountCount.value().mountCount = activityMounts;
        // Protect against division by zero
        if (m->activityNameAndWeightedMountCount.value().weight) {
          m->activityNameAndWeightedMountCount.value().weightedMountCount =
              1.0 * activityMounts / m->activityNameAndWeightedMountCount.value().weight;
        } else {
          m->activityNameAndWeightedMountCount.value().weightedMountCount = std::numeric_limits<double>::max();
        }
      }
      log::ScopedParamContainer params(lc);
      params.add("tapePool", m->tapePool);
      params.add("vo",voOfThisPotentialMount.name);
      if ( m->type == common::dataStructures::MountType::Retrieve) {
        params.add("tapeVid", m->vid);
      }
      params.add("mountType", common::dataStructures::toString(m->type))
            .add("existingMountsDistinctTypesForThisTapepool", existingMountsDistinctTypesForThisTapepool)
            .add("existingMountsBasicTypeForThisVo",existingMountsBasicTypeForThisVo)
            .add("bytesQueued", m->bytesQueued)
            .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
            .add("filesQueued", m->filesQueued)
            .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
            .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
            .add("minArchiveRequestAge", m->minRequestAge)
            .add("maxDrives", maxDrives)
            .add("voReadMaxDrives",voOfThisPotentialMount.readMaxDrives)
            .add("voWriteMaxDrives",voOfThisPotentialMount.writeMaxDrives)
            .add("ratioOfMountQuotaUsed", m->ratioOfMountQuotaUsed);
      lc.log(log::DEBUG, "In Scheduler::sortAndGetTapesForMountInfo(): Will consider potential mount");
      m++;
   }
  }

  // We can now sort the potential mounts in decreasing priority order.
  // The ordering is defined in operator <.
  // We want the result in descending order or priority so we reverse the vector
  std::sort(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());
  std::reverse(mountInfo->potentialMounts.begin(), mountInfo->potentialMounts.end());

  candidateSortingTime = timer.secs(utils::Timer::resetCounter);

  // Find out if we have any potential archive mount in the list. If so, get the
  // list of tapes from the catalogue.
  if (std::count_if(
        mountInfo->potentialMounts.cbegin(), mountInfo->potentialMounts.cend(),
        [](decltype(*mountInfo->potentialMounts.cbegin())& m){ return common::dataStructures::getMountBasicType(m.type) == common::dataStructures::MountType::ArchiveAllTypes; } )) {
    tapeList = m_catalogue.getTapesForWriting(logicalLibraryName);
    getTapeForWriteTime = timer.secs(utils::Timer::resetCounter);
  }

  // Remove from the tape list the ones already or soon to be mounted
  auto t=tapeList.begin();
  while (t!=tapeList.end()) {
    if (tapesInUse.count(t->vid)) {
      t=tapeList.erase(t);
    } else {
      t++;
    }
  }
}

//------------------------------------------------------------------------------
// getLogicalLibrary
//------------------------------------------------------------------------------
cta::optional<common::dataStructures::LogicalLibrary> Scheduler::getLogicalLibrary(const std::string& libraryName, double& getLogicalLibraryTime){
  utils::Timer timer;
  auto logicalLibraries = m_catalogue.getLogicalLibraries();
  cta::optional<common::dataStructures::LogicalLibrary> ret;
  auto logicalLibraryItor = std::find_if(logicalLibraries.begin(),logicalLibraries.end(),[libraryName](const cta::common::dataStructures::LogicalLibrary& ll){
    return (ll.name == libraryName);
  });
  getLogicalLibraryTime += timer.secs(utils::Timer::resetCounter);
  if(logicalLibraryItor != logicalLibraries.end()){
    ret = *logicalLibraryItor;
  }
  return ret;
}

void Scheduler::deleteRepackBuffer(std::unique_ptr<cta::disk::Directory> repackBuffer, cta::log::LogContext & lc) {
  try{
    if(repackBuffer != nullptr && repackBuffer->exist()){
      repackBuffer->rmdir();
    }
  } catch (const cta::exception::XrootCl & ex) {
    log::ScopedParamContainer spc(lc);
    spc.add("exceptionMsg",ex.getMessageValue());
    lc.log(log::ERR,"In Scheduler::deleteRepackBuffer() unable to delete the directory located in " + repackBuffer->getURL());
  }
}

uint64_t Scheduler::getNbFilesAlreadyArchived(const common::dataStructures::ArchiveFile& archiveFile) {
  return archiveFile.tapeFiles.size();
}


//------------------------------------------------------------------------------
// checkNeededEnvironmentVariables
//------------------------------------------------------------------------------
void Scheduler::checkNeededEnvironmentVariables(){
  std::set<std::string> environmentVariablesNotSet;
  for(auto & environmentVariable: c_mandatoryEnvironmentVariables){
    std::string envVar = cta::utils::getEnv(environmentVariable);
    if(envVar.empty()){
      environmentVariablesNotSet.insert(environmentVariable);
    }
  }
  if(!environmentVariablesNotSet.empty()){
    std::string listVariablesNotSet = "";
    bool isFirst = true;
    for(auto & environmentVariableNotSet: environmentVariablesNotSet){
      if(isFirst){
        listVariablesNotSet += "[" + environmentVariableNotSet;
        isFirst = false;
      } else {
        listVariablesNotSet += ", " + environmentVariableNotSet;
      }
    };
    listVariablesNotSet += "]";
    std::string errMsg = "In Scheduler::checkNeededEnvironmentVariables(), the following environment variables: "+listVariablesNotSet+" are not set.";
    throw cta::exception::Exception(errMsg);
  }
}

//------------------------------------------------------------------------------
// getNextMountDryRun
//------------------------------------------------------------------------------
bool Scheduler::getNextMountDryRun(const std::string& logicalLibraryName, const std::string& driveName, log::LogContext& lc) {
  // We run the same algorithm as the actual getNextMount without the global lock
  // For this reason, we just return true as soon as valid mount has been found.
  utils::Timer timer;
  double getMountInfoTime = 0;
  double getTapeInfoTime = 0;
  double candidateSortingTime = 0;
  double getTapeForWriteTime = 0;
  double decisionTime = 0;
  double schedulerDbTime = 0;
  double catalogueTime = 0;
  double getLogicalLibrariesTime = 0;

  auto logicalLibrary = getLogicalLibrary(logicalLibraryName,getLogicalLibrariesTime);
  if(logicalLibrary){
    if(logicalLibrary.value().isDisabled){
      log::ScopedParamContainer params(lc);
      params.add("logicalLibrary",logicalLibraryName)
            .add("catalogueTime",getLogicalLibrariesTime);
      lc.log(log::INFO,"In Scheduler::getNextMountDryRun(): logicalLibrary is disabled");
      return false;
    }
  } else {
    log::ScopedParamContainer params(lc);
    params.add("logicalLibrary",logicalLibraryName)
          .add("catalogueTime",getLogicalLibrariesTime);
    lc.log(log::INFO,"In Scheduler::getNextMountDryRun(): logicalLibrary does not exist");
    return false;
  }

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfoNoLock(SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
  getMountInfoTime = timer.secs(utils::Timer::resetCounter);
  ExistingMountSummaryPerTapepool existingMountsDistinctTypeSummaryPerTapepool;
  ExistingMountSummaryPerVo existingMountBasicTypeSummaryPerVo;
  std::set<std::string> tapesInUse;
  std::list<catalogue::TapeForWriting> tapeList;

  sortAndGetTapesForMountInfo(mountInfo, logicalLibraryName, driveName, timer,
      existingMountsDistinctTypeSummaryPerTapepool, existingMountBasicTypeSummaryPerVo, tapesInUse, tapeList,
      getTapeInfoTime, candidateSortingTime, getTapeForWriteTime, lc);

  // We can now simply iterate on the candidates until we manage to find a valid mount
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (common::dataStructures::getMountBasicType(m->type)==common::dataStructures::MountType::ArchiveAllTypes) {
      // We need to find a tape for archiving. It should be both in the right
      // tape pool and in the drive's logical library
      // The first tape matching will go for a prototype.
      // TODO: improve to reuse already partially written tapes and randomization
      for (auto & t: tapeList) {
        if (t.tapePool == m->tapePool) {
          // We have our tape. That's enough.
          decisionTime += timer.secs(utils::Timer::resetCounter);
          schedulerDbTime = getMountInfoTime;
          catalogueTime = getTapeInfoTime + getTapeForWriteTime;
          uint32_t existingMountsDistinctTypeForThisTapepool = 0;
          uint32_t existingMountsBasicTypeForThisVo = 0;
          try {
            existingMountsDistinctTypeForThisTapepool=existingMountsDistinctTypeSummaryPerTapepool.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
          } catch (...) {}
          try {
            existingMountsBasicTypeForThisVo=existingMountBasicTypeSummaryPerVo.at(VirtualOrganizationMountPair(m->vo,common::dataStructures::getMountBasicType(m->type))).totalMounts;
          } catch(...) {}
          log::ScopedParamContainer params(lc);
          params.add("tapePool", m->tapePool)
                .add("tapeVid", t.vid)
                .add("mountType", common::dataStructures::toString(m->type))
                .add("existingMountsDistinctTypeForThisTapepool", existingMountsDistinctTypeForThisTapepool)
                .add("existingMountsBasicTypeForThisVo", existingMountsBasicTypeForThisVo)
                .add("bytesQueued", m->bytesQueued)
                .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
                .add("filesQueued", m->filesQueued)
                .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
                .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
                .add("minArchiveRequestAge", m->minRequestAge)
                .add("getMountInfoTime", getMountInfoTime)
                .add("getTapeInfoTime", getTapeInfoTime)
                .add("candidateSortingTime", candidateSortingTime)
                .add("getTapeForWriteTime", getTapeForWriteTime)
                .add("decisionTime", decisionTime)
                .add("schedulerDbTime", schedulerDbTime)
                .add("catalogueTime", catalogueTime);
          lc.log(log::INFO, "In Scheduler::getNextMountDryRun(): Found a potential mount (archive)");
          return true;
        }
      }
    } else if (m->type==common::dataStructures::MountType::Retrieve) {
      // We know the tape we intend to mount. We have to validate the tape is
      // actually available to read (not mounted or about to be mounted, and pass
      // on it if so).
      if (tapesInUse.count(m->vid)) continue;
      decisionTime += timer.secs(utils::Timer::resetCounter);
      log::ScopedParamContainer params(lc);
      uint32_t existingMountsDistinctTypeForThisTapepool = 0;
      uint32_t existingMountsBasicTypeForThisVo = 0;
      try {
        existingMountsDistinctTypeForThisTapepool=existingMountsDistinctTypeSummaryPerTapepool.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
      } catch (...) {}
      try {
        existingMountsBasicTypeForThisVo=existingMountBasicTypeSummaryPerVo.at(VirtualOrganizationMountPair(m->vo,common::dataStructures::getMountBasicType(m->type))).totalMounts;
      } catch(...) {}
      schedulerDbTime = getMountInfoTime;
      catalogueTime = getTapeInfoTime + getTapeForWriteTime;
      params.add("tapePool", m->tapePool)
            .add("tapeVid", m->vid)
            .add("mountType", common::dataStructures::toString(m->type))
            .add("existingMountsDistinctTypeForThisTapepool", existingMountsDistinctTypeForThisTapepool)
            .add("existingMountsBasicTypeForThisVo", existingMountsBasicTypeForThisVo);
      if (m->activityNameAndWeightedMountCount) {
        params.add("activity", m->activityNameAndWeightedMountCount.value().activity)
              .add("activityMounts", m->activityNameAndWeightedMountCount.value().weightedMountCount)
              .add("ActivityMountCount", m->activityNameAndWeightedMountCount.value().mountCount)
              .add("ActivityWeight", m->activityNameAndWeightedMountCount.value().weight);
      }
      params.add("bytesQueued", m->bytesQueued)
            .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
            .add("filesQueued", m->filesQueued)
            .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
            .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
            .add("minArchiveRequestAge", m->minRequestAge)
            .add("getMountInfoTime", getMountInfoTime)
            .add("getTapeInfoTime", getTapeInfoTime)
            .add("candidateSortingTime", candidateSortingTime)
            .add("getTapeForWriteTime", getTapeForWriteTime)
            .add("decisionTime", decisionTime)
            .add("schedulerDbTime", schedulerDbTime)
            .add("catalogueTime", catalogueTime);
      lc.log(log::INFO, "In Scheduler::getNextMountDryRun(): Found a potential mount (retrieve)");
      return true;
    }
  }
  schedulerDbTime = getMountInfoTime;
  catalogueTime = getTapeInfoTime + getTapeForWriteTime + getLogicalLibrariesTime;
  decisionTime += timer.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params(lc);
  params.add("getMountInfoTime", getMountInfoTime)
        .add("getTapeInfoTime", getTapeInfoTime)
        .add("candidateSortingTime", candidateSortingTime)
        .add("getTapeForWriteTime", getTapeForWriteTime)
        .add("decisionTime", decisionTime)
        .add("schedulerDbTime", schedulerDbTime)
        .add("catalogueTime", catalogueTime);
  if ((getMountInfoTime > 1) || (getTapeInfoTime > 1) || (candidateSortingTime > 1) || (getTapeForWriteTime > 1) ||
      (decisionTime > 1) || (schedulerDbTime > 1) || (catalogueTime > 1))
    lc.log(log::DEBUG, "In Scheduler::getNextMountDryRun(): No valid mount found.");
  return false;
}


//------------------------------------------------------------------------------
// getNextMount
//------------------------------------------------------------------------------
std::unique_ptr<TapeMount> Scheduler::getNextMount(const std::string &logicalLibraryName, const std::string &driveName, log::LogContext & lc) {
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
  utils::Timer timer;
  double getMountInfoTime = 0;
  double queueTrimingTime = 0;
  double getTapeInfoTime = 0;
  double candidateSortingTime = 0;
  double getTapeForWriteTime = 0;
  double decisionTime = 0;
  double mountCreationTime = 0;
  double driveStatusSetTime = 0;
  double schedulerDbTime = 0;
  double getLogicalLibrariesTime = 0;
  double catalogueTime = 0;

auto logicalLibrary = getLogicalLibrary(logicalLibraryName,getLogicalLibrariesTime);
  if(logicalLibrary){
    if(logicalLibrary.value().isDisabled){
      log::ScopedParamContainer params(lc);
      params.add("logicalLibrary",logicalLibraryName)
            .add("catalogueTime",getLogicalLibrariesTime);
      lc.log(log::INFO,"In Scheduler::getNextMount(): logicalLibrary is disabled");
      return std::unique_ptr<TapeMount>();
    }
  } else {
    log::ScopedParamContainer params(lc);
    params.add("logicalLibrary",logicalLibraryName)
          .add("catalogueTime",getLogicalLibrariesTime);
    lc.log(log::CRIT,"In Scheduler::getNextMount(): logicalLibrary does not exist");
    return std::unique_ptr<TapeMount>();
  }

  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfo(lc);
  getMountInfoTime = timer.secs(utils::Timer::resetCounter);
  if (mountInfo->queueTrimRequired) {
    m_db.trimEmptyQueues(lc);
    queueTrimingTime = timer.secs(utils::Timer::resetCounter);
  }
  __attribute__((unused)) SchedulerDatabase::TapeMountDecisionInfo & debugMountInfo = *mountInfo;

  ExistingMountSummaryPerTapepool existingMountsDistinctTypeSummaryPerTapepool;
  ExistingMountSummaryPerVo existingMountBasicTypeSummaryPerVo;
  std::set<std::string> tapesInUse;
  std::list<catalogue::TapeForWriting> tapeList;

  sortAndGetTapesForMountInfo(mountInfo, logicalLibraryName, driveName, timer,
      existingMountsDistinctTypeSummaryPerTapepool, existingMountBasicTypeSummaryPerVo, tapesInUse, tapeList,
      getTapeInfoTime, candidateSortingTime, getTapeForWriteTime, lc);

  // We can now simply iterate on the candidates until we manage to create a
  // mount for one of them
  for (auto m = mountInfo->potentialMounts.begin(); m!=mountInfo->potentialMounts.end(); m++) {
    // If the mount is an archive, we still have to find a tape.
    if (common::dataStructures::getMountBasicType(m->type)==common::dataStructures::MountType::ArchiveAllTypes) {
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
            decisionTime += timer.secs(utils::Timer::resetCounter);
            internalRet->m_dbMount.reset(mountInfo->createArchiveMount(m->type, t,
                driveName,
                logicalLibraryName,
                utils::getShortHostname(),
                t.vo,
                t.mediaType,
                t.vendor,
                t.capacityInBytes,
                time(NULL)).release());
            mountCreationTime += timer.secs(utils::Timer::resetCounter);
            internalRet->m_sessionRunning = true;
            driveStatusSetTime += timer.secs(utils::Timer::resetCounter);
            log::ScopedParamContainer params(lc);
            uint32_t existingMountsDistinctTypeForThisTapepool = 0;
            uint32_t existingMountsBasicTypeForThisVo = 0;
            try {
              existingMountsDistinctTypeForThisTapepool=existingMountsDistinctTypeSummaryPerTapepool.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
            } catch (...) {}
            try {
              existingMountsBasicTypeForThisVo=existingMountBasicTypeSummaryPerVo.at(VirtualOrganizationMountPair(m->vo,common::dataStructures::getMountBasicType(m->type))).totalMounts;
            } catch(...) {}
            schedulerDbTime = getMountInfoTime + queueTrimingTime + mountCreationTime + driveStatusSetTime;
            catalogueTime = getTapeInfoTime + getTapeForWriteTime;

            params.add("tapePool", m->tapePool)
                  .add("tapeVid", t.vid)
                  .add("vo",t.vo)
                  .add("mediaType",t.mediaType)
                  .add("vendor",t.vendor)
                  .add("mountType", common::dataStructures::toString(m->type))
                  .add("existingMountsDistinctTypeForThisTapepool", existingMountsDistinctTypeForThisTapepool)
                  .add("existingMountsBasicTypeForThisVo",existingMountsBasicTypeForThisVo)
                  .add("bytesQueued", m->bytesQueued)
                  .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
                  .add("filesQueued", m->filesQueued)
                  .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
                  .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
                  .add("minArchiveRequestAge", m->minRequestAge)
                  .add("getMountInfoTime", getMountInfoTime)
                  .add("queueTrimingTime", queueTrimingTime)
                  .add("getTapeInfoTime", getTapeInfoTime)
                  .add("candidateSortingTime", candidateSortingTime)
                  .add("getTapeForWriteTime", getTapeForWriteTime)
                  .add("decisionTime", decisionTime)
                  .add("mountCreationTime", mountCreationTime)
                  .add("driveStatusSetTime", driveStatusSetTime)
                  .add("schedulerDbTime", schedulerDbTime)
                  .add("catalogueTime", catalogueTime);
            lc.log(log::INFO, "In Scheduler::getNextMount(): Selected next mount (archive)");
            return std::unique_ptr<TapeMount> (internalRet.release());
          } catch (cta::exception::Exception & ex) {
            log::ScopedParamContainer params(lc);
            params.add("Message", ex.getMessage().str());
            lc.log(log::WARNING, "In Scheduler::getNextMount(): got an exception trying to schedule an archive mount. Trying others.");
            continue;
          }
        }
      }
    } else if (m->type==common::dataStructures::MountType::Retrieve) {
      // We know the tape we intend to mount. We have to validate the tape is
      // actually available to read (not mounted or about to be mounted, and pass
      // on it if so).
      if (tapesInUse.count(m->vid)) continue;
      try {
        // create the mount, and populate its DB side.
        decisionTime += timer.secs(utils::Timer::resetCounter);
        optional<common::dataStructures::DriveState::ActivityAndWeight> actvityAndWeight;
        if (m->activityNameAndWeightedMountCount) {
          actvityAndWeight = common::dataStructures::DriveState::ActivityAndWeight{
            m->activityNameAndWeightedMountCount.value().activity,
            m->activityNameAndWeightedMountCount.value().weight };
        }
        std::unique_ptr<RetrieveMount> internalRet(new RetrieveMount(m_catalogue));
        internalRet->m_dbMount.reset(mountInfo->createRetrieveMount(m->vid,
            m->tapePool,
            driveName,
            logicalLibraryName,
            utils::getShortHostname(),
            m->vo,
            m->mediaType,
            m->vendor,
            m->capacityInBytes,
            time(NULL), actvityAndWeight).release());
        mountCreationTime += timer.secs(utils::Timer::resetCounter);
        internalRet->m_sessionRunning = true;
        internalRet->m_diskRunning = true;
        internalRet->m_tapeRunning = true;
        driveStatusSetTime += timer.secs(utils::Timer::resetCounter);
        log::ScopedParamContainer params(lc);
        uint32_t existingMountsDistinctTypeForThisTapepool = 0;
        uint32_t existingMountsBasicTypeForThisVo = 0;
        try {
          existingMountsDistinctTypeForThisTapepool=existingMountsDistinctTypeSummaryPerTapepool.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
        } catch (...) {}
        try {
          existingMountsBasicTypeForThisVo=existingMountBasicTypeSummaryPerVo.at(VirtualOrganizationMountPair(m->vo,common::dataStructures::getMountBasicType(m->type))).totalMounts;
        } catch(...) {}
        schedulerDbTime = getMountInfoTime + queueTrimingTime + mountCreationTime + driveStatusSetTime;
        catalogueTime = getTapeInfoTime + getTapeForWriteTime;
        params.add("tapePool", m->tapePool)
              .add("tapeVid", m->vid)
              .add("vo",m->vo)
              .add("mediaType",m->mediaType)
              .add("vendor",m->vendor)
              .add("mountType", common::dataStructures::toString(m->type))
              .add("existingMountsDistinctTypeForThisTapepool", existingMountsDistinctTypeForThisTapepool)
              .add("existingMountsBasicTypeForThisVo",existingMountsBasicTypeForThisVo);
        if (m->activityNameAndWeightedMountCount) {
          params.add("activity", m->activityNameAndWeightedMountCount.value().activity)
                .add("activityMounts", m->activityNameAndWeightedMountCount.value().weightedMountCount)
                .add("ActivityMountCount", m->activityNameAndWeightedMountCount.value().mountCount)
                .add("ActivityWeight", m->activityNameAndWeightedMountCount.value().weight);
        }
        params.add("bytesQueued", m->bytesQueued)
              .add("bytesQueued", m->bytesQueued)
              .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
              .add("filesQueued", m->filesQueued)
              .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
              .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
              .add("minArchiveRequestAge", m->minRequestAge)
              .add("getMountInfoTime", getMountInfoTime)
              .add("queueTrimingTime", queueTrimingTime)
              .add("getTapeInfoTime", getTapeInfoTime)
              .add("candidateSortingTime", candidateSortingTime)
              .add("getTapeForWriteTime", getTapeForWriteTime)
              .add("decisionTime", decisionTime)
              .add("mountCreationTime", mountCreationTime)
              .add("driveStatusSetTime", driveStatusSetTime)
              .add("schedulerDbTime", schedulerDbTime)
              .add("catalogueTime", catalogueTime);
        lc.log(log::INFO, "In Scheduler::getNextMount(): Selected next mount (retrieve)");
        return std::unique_ptr<TapeMount> (internalRet.release());
      } catch (exception::Exception & ex) {
        log::ScopedParamContainer params(lc);
        params.add("Message", ex.getMessage().str());
        lc.log(log::WARNING, "In Scheduler::getNextMount(): got an exception trying to schedule a retrieve mount. Trying others.");
        continue;
      }
    } else {
      throw std::runtime_error("In Scheduler::getNextMount unexpected mount type");
    }
  }
  schedulerDbTime = getMountInfoTime + queueTrimingTime + mountCreationTime + driveStatusSetTime;
  catalogueTime = getTapeInfoTime + getTapeForWriteTime + getLogicalLibrariesTime;
  decisionTime += timer.secs(utils::Timer::resetCounter);
  log::ScopedParamContainer params(lc);
  params.add("getMountInfoTime", getMountInfoTime)
        .add("queueTrimingTime", queueTrimingTime)
        .add("getTapeInfoTime", getTapeInfoTime)
        .add("candidateSortingTime", candidateSortingTime)
        .add("getTapeForWriteTime", getTapeForWriteTime)
        .add("decisionTime", decisionTime)
        .add("mountCreationTime", mountCreationTime)
        .add("driveStatusSetTime", driveStatusSetTime)
        .add("schedulerDbTime", schedulerDbTime)
        .add("catalogueTime", catalogueTime);
  lc.log(log::DEBUG, "In Scheduler::getNextMount(): No valid mount found.");
  return std::unique_ptr<TapeMount>();
}

//------------------------------------------------------------------------------
// getSchedulingInformations
//------------------------------------------------------------------------------
std::list<SchedulingInfos> Scheduler::getSchedulingInformations(log::LogContext& lc) {

  std::list<SchedulingInfos> ret;

  utils::Timer timer;
  double getTapeInfoTime = 0;
  double candidateSortingTime = 0;
  double getTapeForWriteTime = 0;

  ExistingMountSummaryPerTapepool existingMountsDistinctTypeSummaryPerTapepool;
  ExistingMountSummaryPerVo existingMountBasicTypeSummaryPerVo;
  std::set<std::string> tapesInUse;
  std::list<catalogue::TapeForWriting> tapeList;

  //get all drive informations and sort them by logical library name
  cta::common::dataStructures::SecurityIdentity admin;
  auto drives = getDriveStates(admin,lc);

  std::map<std::string,std::list<std::string>> logicalLibraryDriveNamesMap;
  for(auto & drive : drives){
    logicalLibraryDriveNamesMap[drive.logicalLibrary].push_back(drive.driveName);
  }

  for(auto & kv: logicalLibraryDriveNamesMap){
     //get mount informations
    std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
    mountInfo = m_db.getMountInfoNoLock(cta::SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT,lc);
    std::string logicalLibrary = kv.first;
    cta::SchedulingInfos schedulingInfos(logicalLibrary);
    for(auto & driveName: kv.second){
      sortAndGetTapesForMountInfo(mountInfo, logicalLibrary, driveName, timer,
      existingMountsDistinctTypeSummaryPerTapepool, existingMountBasicTypeSummaryPerVo, tapesInUse, tapeList,
      getTapeInfoTime, candidateSortingTime, getTapeForWriteTime, lc);
      //schedulingInfos.addDrivePotentialMount
      std::vector<cta::SchedulerDatabase::PotentialMount> potentialMounts = mountInfo->potentialMounts;
      for(auto & potentialMount: potentialMounts){
        schedulingInfos.addPotentialMount(potentialMount);
      }
    }
    if(!schedulingInfos.getPotentialMounts().empty()){
      ret.push_back(schedulingInfos);
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
// getQueuesAndMountSummaries
//------------------------------------------------------------------------------
std::list<common::dataStructures::QueueAndMountSummary> Scheduler::getQueuesAndMountSummaries(log::LogContext& lc) {
  std::list<common::dataStructures::QueueAndMountSummary> ret;

  // Extract relevant information from the object store.
  utils::Timer schedulerDbTimer;
  auto mountDecisionInfo=m_db.getMountInfoNoLock(SchedulerDatabase::PurposeGetMountInfo::SHOW_QUEUES,lc);
  const auto schedulerDbTime = schedulerDbTimer.secs();
  auto & mdi __attribute__((unused)) = *mountDecisionInfo;

  std::set<std::string> tapesWithAQueue;
  for (const auto & pm: mountDecisionInfo->potentialMounts) {
    if(!pm.vid.empty()) {
      tapesWithAQueue.emplace(pm.vid);
    }
  }
  for (const auto & em: mountDecisionInfo->existingOrNextMounts) {
    if (!em.vid.empty()) {
      tapesWithAQueue.emplace(em.vid);
    }
  }

  // Obtain a map of vids to tape info from the catalogue
  utils::Timer catalogueVidToLogicalLibraryTimer;
  const auto vid_to_logical_library = m_catalogue.getVidToLogicalLibrary(tapesWithAQueue);
  const auto catalogueVidToLogicalLibraryTime = catalogueVidToLogicalLibraryTimer.secs();

  for (auto & pm: mountDecisionInfo->potentialMounts) {
    // Find or create the relevant entry.
    auto &summary = common::dataStructures::QueueAndMountSummary::getOrCreateEntry(ret, pm.type, pm.tapePool, pm.vid, vid_to_logical_library);
    switch (pm.type) {
    case common::dataStructures::MountType::ArchiveForUser:
    case common::dataStructures::MountType::ArchiveForRepack:
      summary.mountPolicy.archivePriority = pm.priority;
      summary.mountPolicy.archiveMinRequestAge = pm.minRequestAge;
      summary.bytesQueued = pm.bytesQueued;
      summary.filesQueued = pm.filesQueued;
      summary.oldestJobAge = time(nullptr) - pm.oldestJobStartTime ;
      break;
    case common::dataStructures::MountType::Retrieve:
      // TODO: we should remove the retrieveMinRequestAge if it's redundant, or rename pm.minArchiveRequestAge.
      summary.mountPolicy.retrieveMinRequestAge = pm.minRequestAge;
      summary.mountPolicy.retrievePriority = pm.priority;
      summary.bytesQueued = pm.bytesQueued;
      summary.filesQueued = pm.filesQueued;
      summary.oldestJobAge = time(nullptr) - pm.oldestJobStartTime ;
      if (pm.sleepingMount) {
        common::dataStructures::QueueAndMountSummary::SleepForSpaceInfo sfsi;
        sfsi.startTime = pm.sleepStartTime;
        sfsi.diskSystemName = pm.diskSystemSleptFor;
        sfsi.sleepTime = pm.sleepTime;
        summary.sleepForSpaceInfo = sfsi;
      }
      break;
    default:
      break;
    }
  }
  for (auto & em: mountDecisionInfo->existingOrNextMounts) {
    auto &summary = common::dataStructures::QueueAndMountSummary::getOrCreateEntry(ret, em.type, em.tapePool, em.vid, vid_to_logical_library);
    switch (em.type) {
    case common::dataStructures::MountType::ArchiveForUser:
    case common::dataStructures::MountType::ArchiveForRepack:
    case common::dataStructures::MountType::Retrieve:
      if (em.currentMount)
        summary.currentMounts++;
      /*else
        summary.nextMounts++;*/
      summary.currentBytes += em.bytesTransferred;
      summary.currentFiles += em.filesTransferred;
      summary.latestBandwidth += em.latestBandwidth;
      break;
    default:
      break;
    }
  }
  mountDecisionInfo.reset();
  double catalogueGetTapePoolTotalTime = 0.0;
  double catalogueGetTapesTotalTime = 0.0;
  double catalogueGetVoTotalTime = 0.0;
  // Add the tape and VO information where useful (archive queues).
  for (auto & mountOrQueue: ret) {
    if (common::dataStructures::MountType::ArchiveForUser==mountOrQueue.mountType || common::dataStructures::MountType::ArchiveForRepack==mountOrQueue.mountType) {
      utils::Timer catalogueGetTapePoolTimer;
      const auto tapePool = m_catalogue.getTapePool(mountOrQueue.tapePool);
      catalogueGetTapePoolTotalTime += catalogueGetTapePoolTimer.secs();
      if (tapePool) {
        utils::Timer catalogueGetVoTimer;
        const auto vo = m_catalogue.getCachedVirtualOrganizationOfTapepool(tapePool->name);
        catalogueGetVoTotalTime += catalogueGetVoTimer.secs();
        mountOrQueue.vo = vo.name;
        mountOrQueue.readMaxDrives = vo.readMaxDrives;
        mountOrQueue.writeMaxDrives = vo.writeMaxDrives;
        mountOrQueue.tapesCapacity = tapePool->capacityBytes;
        mountOrQueue.filesOnTapes = tapePool->nbPhysicalFiles;
        mountOrQueue.dataOnTapes = tapePool->dataBytes;
        mountOrQueue.fullTapes = tapePool->nbFullTapes;
        mountOrQueue.writableTapes = tapePool->nbWritableTapes;
      }
    } else if (common::dataStructures::MountType::Retrieve==mountOrQueue.mountType) {
      // Get info for this tape.
      cta::catalogue::TapeSearchCriteria tsc;
      tsc.vid = mountOrQueue.vid;
      utils::Timer catalogueGetTapesTimer;
      auto tapes=m_catalogue.getTapes(tsc);
      catalogueGetTapesTotalTime += catalogueGetTapesTimer.secs();
      if (tapes.size() != 1) {
        throw cta::exception::Exception("In Scheduler::getQueuesAndMountSummaries(): got unexpected number of tapes from catalogue for a retrieve.");
      }
      auto &t=tapes.front();
      utils::Timer catalogueGetVoTimer;
      const auto vo = m_catalogue.getCachedVirtualOrganizationOfTapepool(t.tapePoolName);
      catalogueGetVoTotalTime += catalogueGetVoTimer.secs();
      mountOrQueue.vo = vo.name;
      mountOrQueue.readMaxDrives = vo.readMaxDrives;
      mountOrQueue.writeMaxDrives = vo.writeMaxDrives;
      mountOrQueue.tapesCapacity += t.capacityInBytes;
      mountOrQueue.filesOnTapes += t.lastFSeq;
      mountOrQueue.dataOnTapes += t.dataOnTapeInBytes;
      if (t.full) mountOrQueue.fullTapes++;
      if (!t.full && !t.isDisabled()) mountOrQueue.writableTapes++;
      mountOrQueue.tapePool = t.tapePoolName;
    }
  }
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueVidToLogicalLibraryTime", catalogueVidToLogicalLibraryTime)
     .add("schedulerDbTime", schedulerDbTime)
     .add("catalogueGetTapePoolTotalTime", catalogueGetTapePoolTotalTime)
     .add("catalogueGetVoTotalTime",catalogueGetVoTotalTime)
     .add("catalogueGetTapesTotalTime", catalogueGetTapesTotalTime);
  lc.log(log::INFO, "In Scheduler::getQueuesAndMountSummaries(): success.");
  return ret;
}

//------------------------------------------------------------------------------
// getNextArchiveJobsToReportBatch
//------------------------------------------------------------------------------
std::list<std::unique_ptr<ArchiveJob> > Scheduler::getNextArchiveJobsToReportBatch(
  uint64_t filesRequested, log::LogContext& logContext) {
  // We need to go through the queues of archive jobs to report
  std::list<std::unique_ptr<ArchiveJob> > ret;
  // Get the list of jobs to report from the scheduler db
  auto dbRet = m_db.getNextArchiveJobsToReportBatch(filesRequested, logContext);
  for (auto & j: dbRet) {
    ret.emplace_back(new ArchiveJob(nullptr, m_catalogue, j->archiveFile,
        j->srcURL, j->tapeFile));
    ret.back()->m_dbJob.reset(j.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// getArchiveJobsFailedSummary
//------------------------------------------------------------------------------
SchedulerDatabase::JobsFailedSummary Scheduler::getArchiveJobsFailedSummary(log::LogContext &logContext) {
  return m_db.getArchiveJobsFailedSummary(logContext);
}

//------------------------------------------------------------------------------
// getNextRetrieveJobsToReportBatch
//------------------------------------------------------------------------------
std::list<std::unique_ptr<RetrieveJob>> Scheduler::
getNextRetrieveJobsToReportBatch(uint64_t filesRequested, log::LogContext &logContext)
{
  // We need to go through the queues of retrieve jobs to report
  std::list<std::unique_ptr<RetrieveJob>> ret;
  // Get the list of jobs to report from the scheduler db
  auto dbRet = m_db.getNextRetrieveJobsToReportBatch(filesRequested, logContext);
  for (auto &j : dbRet) {
    ret.emplace_back(new RetrieveJob(nullptr, j->retrieveRequest, j->archiveFile, j->selectedCopyNb, PositioningMethod::ByFSeq));

    ret.back()->m_dbJob.reset(j.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// getNextRetrieveJobsFailedBatch
//------------------------------------------------------------------------------
std::list<std::unique_ptr<RetrieveJob>> Scheduler::
getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext &logContext)
{
  // We need to go through the queues of failed retrieve jobs
  std::list<std::unique_ptr<RetrieveJob>> ret;
  // Get the list of failed jobs from the scheduler db
  auto dbRet = m_db.getNextRetrieveJobsFailedBatch(filesRequested, logContext);
  for (auto &j : dbRet) {
    ret.emplace_back(new RetrieveJob(nullptr, j->retrieveRequest, j->archiveFile, j->selectedCopyNb, PositioningMethod::ByFSeq));
    ret.back()->m_dbJob.reset(j.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// getRetrieveJobsFailedSummary
//------------------------------------------------------------------------------
SchedulerDatabase::JobsFailedSummary Scheduler::getRetrieveJobsFailedSummary(log::LogContext &logContext) {
  return m_db.getRetrieveJobsFailedSummary(logContext);
}

//------------------------------------------------------------------------------
// reportArchiveJobsBatch
//------------------------------------------------------------------------------
void Scheduler::reportArchiveJobsBatch(std::list<std::unique_ptr<ArchiveJob> >& archiveJobsBatch,
    disk::DiskReporterFactory & reporterFactory, log::TimingList& timingList, utils::Timer& t,
    log::LogContext& lc){
  // Create the reporters
  struct JobAndReporter {
    std::unique_ptr<disk::DiskReporter> reporter;
    ArchiveJob * archiveJob;
  };
  std::list<JobAndReporter> pendingReports;
  std::list<ArchiveJob *> reportedJobs;
  for (auto &j: archiveJobsBatch) {
    pendingReports.push_back(JobAndReporter());
    auto & current = pendingReports.back();
    // We could fail to create the disk reporter or to get the report URL. This should not impact the other jobs.
    try {
      current.reporter.reset(reporterFactory.createDiskReporter(j->exceptionThrowingReportURL()));
      current.reporter->asyncReport();
      current.archiveJob = j.get();
    } catch (cta::exception::Exception & ex) {
      // Whether creation or launching of reporter failed, the promise will not receive result, so we can safely delete it.
      // we will first determine if we need to clean up the reporter as well or not.
      pendingReports.pop_back();
      // We are ready to carry on for other files without interactions.
      // Log the error, update the request.
      log::ScopedParamContainer params(lc);
      params.add("fileId", j->archiveFile.archiveFileID)
            .add("reportType", j->reportType())
            .add("exceptionMSG", ex.getMessageValue());
      lc.log(log::ERR, "In Scheduler::reportArchiveJobsBatch(): failed to launch reporter.");
      try {
        j->reportFailed(ex.getMessageValue(), lc);
      } catch(const cta::objectstore::Backend::NoSuchObject &ex){
        params.add("fileId",j->archiveFile.archiveFileID)
              .add("reportType",j->reportType())
              .add("exceptionMSG",ex.getMessageValue());
        lc.log(log::WARNING,"In Scheduler::reportArchiveJobsBatch(): failed to reportFailed the job because it does not exist in the objectstore.");
      }
    }
  }
  timingList.insertAndReset("asyncReportLaunchTime", t);
  for (auto &current: pendingReports) {
    try {
      current.reporter->waitReport();
      reportedJobs.push_back(current.archiveJob);
    } catch (cta::exception::Exception & ex) {
      // Log the error, update the request.
      log::ScopedParamContainer params(lc);
      params.add("fileId", current.archiveJob->archiveFile.archiveFileID)
            .add("reportType", current.archiveJob->reportType())
            .add("exceptionMSG", ex.getMessageValue());
      lc.log(log::ERR, "In Scheduler::reportArchiveJobsBatch(): failed to report.");
      try {
        current.archiveJob->reportFailed(ex.getMessageValue(), lc);
      } catch(const cta::objectstore::Backend::NoSuchObject &ex){
        params.add("fileId",current.archiveJob->archiveFile.archiveFileID)
              .add("reportType",current.archiveJob->reportType())
              .add("exceptionMSG",ex.getMessageValue());
        lc.log(log::WARNING,"In Scheduler::reportArchiveJobsBatch(): failed to reportFailed the current job because it does not exist in the objectstore.");
      }
    }
  }
  timingList.insertAndReset("reportCompletionTime", t);
  std::list<SchedulerDatabase::ArchiveJob *> reportedDbJobs;
  for (auto &j: reportedJobs) reportedDbJobs.push_back(j->m_dbJob.get());
  m_db.setArchiveJobBatchReported(reportedDbJobs, timingList, t, lc);
  // Log the successful reports.
  for (auto & j: reportedJobs) {
    log::ScopedParamContainer params(lc);
    params.add("fileId", j->archiveFile.archiveFileID)
          .add("reportType", j->reportType());
    lc.log(log::INFO, "In Scheduler::reportArchiveJobsBatch(): report successful.");
  }
  timingList.insertAndReset("reportRecordingInSchedDbTime", t);
  log::ScopedParamContainer params(lc);
  params.add("totalReports", archiveJobsBatch.size())
        .add("failedReports", archiveJobsBatch.size() - reportedJobs.size())
        .add("successfulReports", reportedJobs.size());
  timingList.addToLog(params);
  lc.log(log::INFO, "In Scheduler::reportArchiveJobsBatch(): reported a batch of archive jobs.");
}

//------------------------------------------------------------------------------
// reportRetrieveJobsBatch
//------------------------------------------------------------------------------
void Scheduler::
reportRetrieveJobsBatch(std::list<std::unique_ptr<RetrieveJob>> & retrieveJobsBatch,
  disk::DiskReporterFactory & reporterFactory, log::TimingList & timingList, utils::Timer & t, log::LogContext & lc)
{
  // Create the reporters
  struct JobAndReporter {
    std::unique_ptr<disk::DiskReporter> reporter;
    RetrieveJob * retrieveJob;
  };
  std::list<JobAndReporter> pendingReports;
  std::list<RetrieveJob*> reportedJobs;
  for(auto &j: retrieveJobsBatch) {
    pendingReports.push_back(JobAndReporter());
    auto & current = pendingReports.back();
    // We could fail to create the disk reporter or to get the report URL. This should not impact the other jobs.
    try {
      current.reporter.reset(reporterFactory.createDiskReporter(j->retrieveRequest.errorReportURL));
      current.reporter->asyncReport();
      current.retrieveJob = j.get();
    } catch (cta::exception::Exception & ex) {
      // Whether creation or launching of reporter failed, the promise will not receive result, so we can safely delete it.
      // we will first determine if we need to clean up the reporter as well or not.
      pendingReports.pop_back();
      // We are ready to carry on for other files without interactions.
      // Log the error, update the request.
      log::ScopedParamContainer params(lc);
      params.add("fileId", j->archiveFile.archiveFileID)
            .add("reportType", j->reportType())
            .add("exceptionMSG", ex.getMessageValue());
      lc.log(log::ERR, "In Scheduler::reportRetrieveJobsBatch(): failed to launch reporter.");
      j->reportFailed(ex.getMessageValue(), lc);
    }
  }
  timingList.insertAndReset("asyncReportLaunchTime", t);
  for(auto &current: pendingReports) {
    try {
      current.reporter->waitReport();
      reportedJobs.push_back(current.retrieveJob);
    } catch (cta::exception::Exception & ex) {
      // Log the error, update the request.
      log::ScopedParamContainer params(lc);
      params.add("fileId", current.retrieveJob->archiveFile.archiveFileID)
            .add("reportType", current.retrieveJob->reportType())
            .add("exceptionMSG", ex.getMessageValue());
      lc.log(log::ERR, "In Scheduler::reportRetrieveJobsBatch(): failed to report.");
      current.retrieveJob->reportFailed(ex.getMessageValue(), lc);
    }
  }
  timingList.insertAndReset("reportCompletionTime", t);
  std::list<SchedulerDatabase::RetrieveJob *> reportedDbJobs;
  for(auto &j: reportedJobs) reportedDbJobs.push_back(j->m_dbJob.get());
  m_db.setRetrieveJobBatchReportedToUser(reportedDbJobs, timingList, t, lc);
  // Log the successful reports.
  for(auto & j: reportedJobs) {
    log::ScopedParamContainer params(lc);
    params.add("fileId", j->archiveFile.archiveFileID)
          .add("reportType", j->reportType());
    lc.log(log::INFO, "In Scheduler::reportRetrieveJobsBatch(): report successful.");
  }
  timingList.insertAndReset("reportRecordingInSchedDbTime", t);
  log::ScopedParamContainer params(lc);
  params.add("totalReports", retrieveJobsBatch.size())
        .add("failedReports", retrieveJobsBatch.size() - reportedJobs.size())
        .add("successfulReports", reportedJobs.size());
  timingList.addToLog(params);
  lc.log(log::ERR, "In Scheduler::reportRetrieveJobsBatch(): reported a batch of retrieve jobs.");
}

cta::catalogue::Catalogue & Scheduler::getCatalogue(){
  return m_catalogue;
}

} // namespace cta
