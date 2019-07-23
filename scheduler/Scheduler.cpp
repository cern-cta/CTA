/*
 * The CERN Tape Archive(CTA) project
 * Copyright (C) 2018 CERN
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
#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"
#include "common/utils/utils.hpp"
#include "common/Timer.hpp"
#include "common/exception/NonRetryableError.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "objectstore/RepackRequest.hpp"
#include "RetrieveRequestDump.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "disk/RadosStriperPool.hpp"
#include "OStoreDB/OStoreDB.hpp"

#include <iostream>
#include <sstream>
#include <iomanip>
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
void Scheduler::ping(log::LogContext & lc) {
  cta::utils::Timer t;
  m_catalogue.ping();
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  m_db.ping();
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime);
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
void Scheduler::queueArchiveWithGivenId(const uint64_t archiveFileId, const std::string &instanceName,
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
     .add("diskFileOwnerUid", request.diskFileInfo.owner_uid)
     .add("diskFileGid", request.diskFileInfo.gid)
     .add("checksumBlob", request.checksumBlob)
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
  lc.log(log::INFO, "Queued archive request");
}

void Scheduler::queueArchiveRequestForRepackBatch(std::list<cta::objectstore::ArchiveRequest> &archiveRequests,log::LogContext &lc)
{
  for(auto& archiveRequest : archiveRequests){
    objectstore::ScopedExclusiveLock rReqL(archiveRequest);
    archiveRequest.fetch();
    cta::common::dataStructures::ArchiveFile archiveFile = archiveRequest.getArchiveFile();
    rReqL.release();
    std::unique_ptr<cta::objectstore::ArchiveRequest>  arUniqPtr = cta::make_unique<cta::objectstore::ArchiveRequest>(archiveRequest);
    this->m_db.queueArchiveForRepack(std::move(arUniqPtr),lc);
    cta::log::TimingList tl;
    utils::Timer t;
    tl.insOrIncAndReset("schedulerDbTime", t);
    log::ScopedParamContainer spc(lc);
    spc.add("instanceName", archiveFile.diskInstance)
     .add("storageClass", archiveFile.storageClass)
     .add("diskFileID", archiveFile.diskFileId)
     .add("fileSize", archiveFile.fileSize)
     .add("fileId", archiveFile.archiveFileID);
    tl.insertOrIncrement("schedulerDbTime",t.secs());
    tl.addToLog(spc);
    lc.log(log::INFO,"Queued repack archive request");
  }
}

//------------------------------------------------------------------------------
// queueRetrieve
//------------------------------------------------------------------------------
void Scheduler::queueRetrieve(
  const std::string &instanceName,
  common::dataStructures::RetrieveRequest &request,
  log::LogContext & lc) {
  using utils::postEllipsis;
  using utils::midEllipsis;
  utils::Timer t;
  // Get the queue criteria
  common::dataStructures::RetrieveFileQueueCriteria queueCriteria;
  queueCriteria = m_catalogue.prepareToRetrieveFile(instanceName, request.archiveFileID, request.requester, request.activity, lc);
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  std::string selectedVid = m_db.queueRetrieve(request, queueCriteria, lc);
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
     .add("criteriaChecksumBlob", queueCriteria.archiveFile.checksumBlob)
     .add("criteriaCreationTime", queueCriteria.archiveFile.creationTime)
     .add("criteriaDiskFileId", queueCriteria.archiveFile.diskFileId)
     .add("criteriaDiskFilePath", queueCriteria.archiveFile.diskFileInfo.path)
     .add("criteriaDiskFileOwnerUid", queueCriteria.archiveFile.diskFileInfo.owner_uid)
     .add("criteriaDiskInstance", queueCriteria.archiveFile.diskInstance)
     .add("criteriaFileSize", queueCriteria.archiveFile.fileSize)
     .add("reconciliationTime", queueCriteria.archiveFile.reconciliationTime)
     .add("storageClass", queueCriteria.archiveFile.storageClass);
  for (auto & tf:queueCriteria.archiveFile.tapeFiles) {
    std::stringstream tc;
    tc << "tapeCopy" << tf.copyNb;
    spc.add(tc.str(), tf);
  }
  spc.add("selectedVid", selectedVid)
     .add("catalogueTime", catalogueTime)
     .add("schedulerDbTime", schedulerDbTime)
     .add("policyName", queueCriteria.mountPolicy.name)
     .add("policyMaxDrives", queueCriteria.mountPolicy.maxDrivesAllowed)
     .add("policyMinAge", queueCriteria.mountPolicy.retrieveMinRequestAge)
     .add("policyPriority", queueCriteria.mountPolicy.retrievePriority);
  if (request.activity)
    spc.add("activity", request.activity.value());
  lc.log(log::INFO, "Queued retrieve request");
}

//------------------------------------------------------------------------------
// deleteArchive
//------------------------------------------------------------------------------
void Scheduler::deleteArchive(const std::string &instanceName, const common::dataStructures::DeleteArchiveRequest &request, 
    log::LogContext & lc) {
  // We have different possible scenarios here. The file can be safe in the catalogue,
  // fully queued, or partially queued.
  // First, make sure the file is not queued anymore.
// TEMPORARILY commenting out SchedulerDatabase::deleteArchiveRequest() in order
// to reduce latency.  PLEASE NOTE however that this means files "in-flight" to
// tape will not be deleted and they will appear in the CTA catalogue when they
// are finally written to tape.
//try {
//  m_db.deleteArchiveRequest(instanceName, request.archiveFileID);
//} catch (exception::Exception &dbEx) {
//  // The file was apparently not queued. If we fail to remove it from the
//  // catalogue for any reason other than it does not exist in the catalogue,
//  // then it is an error.
//  m_catalogue.deleteArchiveFile(instanceName, request.archiveFileID);
//}
  utils::Timer t;
  m_catalogue.deleteArchiveFile(instanceName, request.archiveFileID, lc);
  auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
  log::ScopedParamContainer spc(lc);
  spc.add("catalogueTime", catalogueTime);
  lc.log(log::INFO, "In Scheduler::deleteArchive(): success.");
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
void Scheduler::queueLabel(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force) {
  throw exception::Exception(std::string("Not implemented: ") + __PRETTY_FUNCTION__);
}

void Scheduler::checkTapeFullBeforeRepack(std::string vid){
  std::set<std::string> vidToRepack;
  vidToRepack.insert(vid);
  try{
    auto vidToTapesMap = m_catalogue.getTapesByVid(vidToRepack); //throws an exception if the vid is not found on the database
    cta::common::dataStructures::Tape tapeToCheck = vidToTapesMap.at(vid);
    if(!tapeToCheck.full){
      throw exception::UserError("You must set the tape as full before repacking it.");
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
void Scheduler::queueRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, 
    const std::string & bufferURL, const common::dataStructures::RepackInfo::Type repackType, log::LogContext & lc) {
  // Check request sanity
  if (vid.empty()) throw exception::UserError("Empty VID name.");
  if (bufferURL.empty()) throw exception::UserError("Empty buffer URL.");
  utils::Timer t;
  checkTapeFullBeforeRepack(vid);
  m_db.queueRepack(vid, bufferURL, repackType, lc);
  log::TimingList tl;
  tl.insertAndReset("schedulerDbTime", t);
  log::ScopedParamContainer params(lc);
  params.add("tapeVid", vid)
        .add("repackType", toString(repackType))
        .add("bufferURL", bufferURL);
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
// setRepackRequestExpansionTimeLimit
//------------------------------------------------------------------------------
void Scheduler::setRepackRequestExpansionTimeLimit(const double& time) {
  m_repackRequestExpansionTimeLimit = time;
}

//------------------------------------------------------------------------------
// getRepackRequestExpansionTimeLimit
//------------------------------------------------------------------------------
double Scheduler::getRepackRequestExpansionTimeLimit() const {
  return m_repackRequestExpansionTimeLimit;
}

//------------------------------------------------------------------------------
// expandRepackRequest
//------------------------------------------------------------------------------
void Scheduler::expandRepackRequest(std::unique_ptr<RepackRequest>& repackRequest, log::TimingList& timingList, utils::Timer& t, log::LogContext& lc) {
  std::list<common::dataStructures::ArchiveFile> files;
  auto repackInfo = repackRequest->getRepackInfo();
  
  typedef cta::common::dataStructures::RepackInfo::Type RepackType;
  if (repackInfo.type != RepackType::MoveOnly) {
    log::ScopedParamContainer params(lc);
    params.add("tapeVid", repackInfo.vid);
    lc.log(log::ERR, "In Scheduler::expandRepackRequest(): failing repack request with unsupported (yet) type.");
    repackRequest->fail();
    return;
  }
  //We need to get the ArchiveRoutes to allow the retrieval of the tapePool in which the
  //tape where the file is is located
  std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue.getArchiveRoutes();
  timingList.insertAndReset("catalogueGetArchiveRoutesTime",t);
  //To identify the routes, we need to have both the dist instance name and the storage class name
  //thus, the key of the map is a pair of string
  cta::common::dataStructures::ArchiveRoute::FullMap archiveRoutesMap;
  for(auto route: routes){
    //insert the route into the map to allow a quick retrieval
    archiveRoutesMap[std::make_pair(route.diskInstanceName,route.storageClassName)][route.copyNb] = route;
  }
  uint64_t fSeq;
  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles totalStatsFile;
  repackRequest->m_dbReq->fillLastExpandedFSeqAndTotalStatsFile(fSeq,totalStatsFile);
  timingList.insertAndReset("fillTotalStatsFileBeforeExpandTime",t);
  cta::catalogue::ArchiveFileItor archiveFilesForCatalogue = m_catalogue.getArchiveFilesForRepackItor(repackInfo.vid, fSeq);
  timingList.insertAndReset("catalogueGetArchiveFilesForRepackItorTime",t);
  
  std::stringstream dirBufferURL;
  dirBufferURL << repackInfo.repackBufferBaseURL << "/" << repackInfo.vid << "/";
  std::set<std::string> filesInDirectory;
  if(archiveFilesForCatalogue.hasMore()){
    //We only create the folder if there are some files to Repack
    cta::disk::DirectoryFactory dirFactory;
    std::unique_ptr<cta::disk::Directory> dir;
    dir.reset(dirFactory.createDirectory(dirBufferURL.str()));
    if(dir->exist()){
      filesInDirectory = dir->getFilesName();
    } else {
      dir->mkdir();
    }
  }
  double elapsedTime = 0;
  bool stopExpansion = false;
  repackRequest->m_dbReq->setExpandStartedAndChangeStatus();
  while(archiveFilesForCatalogue.hasMore() && !stopExpansion) {
    size_t filesCount = 0;
    uint64_t maxAddedFSeq = 0;
    std::list<SchedulerDatabase::RepackRequest::Subrequest> retrieveSubrequests;
    while(filesCount < c_defaultMaxNbFilesForRepack && !stopExpansion && archiveFilesForCatalogue.hasMore())
    {
      filesCount++;
      fSeq++;
      retrieveSubrequests.push_back(cta::SchedulerDatabase::RepackRequest::Subrequest());
      auto archiveFile = archiveFilesForCatalogue.next();
      auto & retrieveSubRequest  = retrieveSubrequests.back();
      
      retrieveSubRequest.archiveFile = archiveFile;
      retrieveSubRequest.fSeq = std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max();
      
      // We have to determine which copynbs we want to rearchive, and under which fSeq we record this file.
      if (repackInfo.type == RepackType::MoveAndAddCopies || repackInfo.type == RepackType::MoveOnly) {
        // determine which fSeq(s) (normally only one) lives on this tape.
        for (auto & tc: archiveFile.tapeFiles) if (tc.vid == repackInfo.vid) {
          // We make the (reasonable) assumption that the archive file only has one copy on this tape.
          // If not, we will ensure the subrequest is filed under the lowest fSeq existing on this tape.
          // This will prevent double subrequest creation (we already have such a mechanism in case of crash and 
          // restart of expansion.
          if(tc.supersededByVid.empty()){
            //We want to Archive the "active" copies on the tape, thus the copies that are not superseded by another
            //we want to Retrieve the "active" fSeq
            totalStatsFile.totalFilesToArchive += 1;
            totalStatsFile.totalBytesToArchive += retrieveSubRequest.archiveFile.fileSize;
            retrieveSubRequest.copyNbsToRearchive.insert(tc.copyNb);
            retrieveSubRequest.fSeq = tc.fSeq;
          }
          //retrieveSubRequest.fSeq = (retrieveSubRequest.fSeq == std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max()) ? tc.fSeq : std::max(tc.fSeq, retrieveSubRequest.fSeq);
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
          /*createArchiveSubrequest = true;
          retrieveSubrequests.pop_back();*/
          //TODO : We don't want to retrieve the file again, create archive subrequest
        }
      }
      if(!createArchiveSubrequest){
        totalStatsFile.totalBytesToRetrieve += retrieveSubRequest.archiveFile.fileSize;
        totalStatsFile.totalFilesToRetrieve += 1;
        if (repackInfo.type == RepackType::MoveAndAddCopies || repackInfo.type == RepackType::AddCopiesOnly) {
          // We should not get here are the type is filtered at the beginning of the function.
          // TODO: add support for expand.
          throw cta::exception::Exception("In Scheduler::expandRepackRequest(): expand not yet supported.");
        }
        if ((retrieveSubRequest.fSeq == std::numeric_limits<decltype(retrieveSubRequest.fSeq)>::max()) || retrieveSubRequest.copyNbsToRearchive.empty()) {
          log::ScopedParamContainer params(lc);
          params.add("fileId", retrieveSubRequest.archiveFile.archiveFileID)
                .add("repackVid", repackInfo.vid);
          lc.log(log::ERR, "In Scheduler::expandRepackRequest(): no fSeq found for this file on this tape.");
          retrieveSubrequests.pop_back();
        } else {
          // We found some copies to rearchive. We still have to decide which file path we are going to use.
          // File path will be base URL + /<VID>/<fSeq>
          /*std::stringstream fileBufferURL;
          fileBufferURL << repackInfo.repackBufferBaseURL << "/" << repackInfo.vid << "/" 
              << std::setw(9) << std::setfill('0') << rsr.fSeq;*/
          maxAddedFSeq = std::max(maxAddedFSeq,retrieveSubRequest.fSeq);
          retrieveSubRequest.fileBufferURL = dirBufferURL.str() + fileName.str();
        }
      }
      stopExpansion = (elapsedTime >= m_repackRequestExpansionTimeLimit);
    }
    // Note: the highest fSeq will be recorded internally in the following call.
    // We know that the fSeq processed on the tape are >= initial fSeq + filesCount - 1 (or fSeq - 1 as we counted). 
    // We pass this information to the db for recording in the repack request. This will allow restarting from the right
    // value in case of crash.
    repackRequest->m_dbReq->addSubrequestsAndUpdateStats(retrieveSubrequests, archiveRoutesMap, fSeq, maxAddedFSeq, totalStatsFile, lc);
    timingList.insertAndReset("addSubrequestsAndUpdateStatsTime",t);
    {
      if(!stopExpansion && archiveFilesForCatalogue.hasMore()){
        log::ScopedParamContainer params(lc);
        params.add("tapeVid",repackInfo.vid);
        timingList.addToLog(params);
        lc.log(log::INFO,"Max number of files expanded reached ("+std::to_string(c_defaultMaxNbFilesForRepack)+"), doing some reporting before continuing expansion.");
      }
    }
  }
  log::ScopedParamContainer params(lc);
  params.add("tapeVid",repackInfo.vid);
  timingList.addToLog(params);
  if(archiveFilesForCatalogue.hasMore()){
    if(stopExpansion){
      repackRequest->m_dbReq->requeueInToExpandQueue(lc);
      lc.log(log::INFO,"Expansion time reached, Repack Request requeued in ToExpand queue.");
    }
  } else {
    repackRequest->m_dbReq->expandDone();
    lc.log(log::INFO,"In Scheduler::expandRepackRequest(), repack request expanded");
  }
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
  auto driveStates = m_db.getDriveStates(lc);
  for (auto & d: driveStates) {
    if (d.driveName == driveName) {
      auto schedulerDbTime = t.secs();
      if (schedulerDbTime > 1) {
        log::ScopedParamContainer spc(lc);
        spc.add("drive", driveName)
           .add("schedulerDbTime", schedulerDbTime);
        lc.log(log::INFO, "In Scheduler::getDesiredDriveState(): success.");
      }
      return d.desiredDriveState;
    }
  }
  throw NoSuchDrive ("In Scheduler::getDesiredDriveState(): no such drive");
}

//------------------------------------------------------------------------------
// setDesiredDriveState
//------------------------------------------------------------------------------
void Scheduler::setDesiredDriveState(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force, log::LogContext & lc) {
  utils::Timer t;
  common::dataStructures::DesiredDriveState desiredDriveState;
  desiredDriveState.up = up;
  desiredDriveState.forceDown = force;
  m_db.setDesiredDriveState(driveName, desiredDriveState, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveName)
     .add("up", up?"up":"down")
     .add("force", force?"yes":"no")
     .add("schedulerDbTime", schedulerDbTime);
   lc.log(log::INFO, "In Scheduler::setDesiredDriveState(): success.");   
}

//------------------------------------------------------------------------------
// removeDrive
//------------------------------------------------------------------------------
void Scheduler::removeDrive(const common::dataStructures::SecurityIdentity &cliIdentity, 
  const std::string &driveName, log::LogContext & lc) {
  utils::Timer t;
  m_db.removeDrive(driveName, lc);
  auto schedulerDbTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("drive", driveName)
     .add("schedulerDbTime", schedulerDbTime);
  lc.log(log::INFO, "In Scheduler::removeDrive(): success.");   
}

//------------------------------------------------------------------------------
// setDesiredDriveState
//------------------------------------------------------------------------------
void Scheduler::reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo, common::dataStructures::MountType type, common::dataStructures::DriveStatus status, log::LogContext & lc) {
  // TODO: mount type should be transmitted too.
  utils::Timer t;
  m_db.reportDriveStatus(driveInfo, type, status, time(NULL), lc);
  auto schedulerDbTime = t.secs();
  if (schedulerDbTime > 1) {
    log::ScopedParamContainer spc(lc);
    spc.add("drive", driveInfo.driveName)
       .add("schedulerDbTime", schedulerDbTime);
    lc.log(log::INFO, "In Scheduler::reportDriveStatus(): success.");
  }
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
std::list<common::dataStructures::DriveState> Scheduler::getDriveStates(const common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc) const {
  utils::Timer t;
  auto ret = m_db.getDriveStates(lc);
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
    ExistingMountSummary & existingMountsSummary, std::set<std::string> & tapesInUse, std::list<catalogue::TapeForWriting> & tapeList,
    double & getTapeInfoTime, double & candidateSortingTime, double & getTapeForWriteTime, log::LogContext & lc) {
  // The library information is not know for the tapes involved in retrieves. We 
  // need to query the catalogue now about all those tapes.
  // Build the list of tapes.
  std::set<std::string> tapeSet;
  for (auto &m:mountInfo->potentialMounts) {
    if (m.type==common::dataStructures::MountType::Retrieve) tapeSet.insert(m.vid);
  }
  if (tapeSet.size()) {
    auto tapesInfo=m_catalogue.getTapesByVid(tapeSet);
    getTapeInfoTime = timer.secs(utils::Timer::resetCounter);
    for (auto &m:mountInfo->potentialMounts) {
      if (m.type==common::dataStructures::MountType::Retrieve) {
        m.logicalLibrary=tapesInfo[m.vid].logicalLibraryName;
        m.tapePool=tapesInfo[m.vid].tapePoolName;
        m.vendor = tapesInfo[m.vid].vendor;
        m.mediaType = tapesInfo[m.vid].mediaType;
        m.vo = tapesInfo[m.vid].vo;
        m.capacityInBytes = tapesInfo[m.vid].capacityInBytes;
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
  for (auto & em: mountInfo->existingOrNextMounts) {
    // If a mount is still listed for our own drive, it is a leftover that we disregard.
    if (em.driveName!=driveName) {
      existingMountsSummary[TapePoolMountPair(em.tapePool, common::dataStructures::getMountBasicType(em.type))].totalMounts++;
      if (em.activity)
        existingMountsSummary[TapePoolMountPair(em.tapePool, common::dataStructures::getMountBasicType(em.type))]
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
  // quota is already reached, and weight the remaining by how much of their quota 
  // is reached
  for (auto m = mountInfo->potentialMounts.begin(); m!= mountInfo->potentialMounts.end();) {
    // Get summary data
    uint32_t existingMounts = 0;
    uint32_t activityMounts = 0;
    try {
      existingMounts = existingMountsSummary
          .at(TapePoolMountPair(m->tapePool, common::dataStructures::getMountBasicType(m->type)))
             .totalMounts;
    } catch (std::out_of_range &) {}
    if (m->activityNameAndWeightedMountCount) {
      try {
        activityMounts = existingMountsSummary
          .at(TapePoolMountPair(m->tapePool, common::dataStructures::getMountBasicType(m->type)))
             .activityMounts.at(m->activityNameAndWeightedMountCount.value().activity).value;
      } catch (std::out_of_range &) {}
    }
    uint32_t effectiveExistingMounts = 0;
    if (m->type == common::dataStructures::MountType::ArchiveForUser) effectiveExistingMounts = existingMounts;
    bool mountPassesACriteria = false;
    
    if (m->bytesQueued / (1 + effectiveExistingMounts) >= m_minBytesToWarrantAMount)
      mountPassesACriteria = true;
    if (m->filesQueued / (1 + effectiveExistingMounts) >= m_minFilesToWarrantAMount)
      mountPassesACriteria = true;
    if (!effectiveExistingMounts && ((time(NULL) - m->oldestJobStartTime) > m->minRequestAge))
      mountPassesACriteria = true;
    if (!mountPassesACriteria || existingMounts >= m->maxDrivesAllowed) {
      log::ScopedParamContainer params(lc);
      params.add("tapePool", m->tapePool);
      if ( m->type == common::dataStructures::MountType::Retrieve) {
        params.add("tapeVid", m->vid);
      }
      params.add("mountType", common::dataStructures::toString(m->type))
            .add("existingMounts", existingMounts)
            .add("bytesQueued", m->bytesQueued)
            .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
            .add("filesQueued", m->filesQueued)
            .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
            .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
            .add("minArchiveRequestAge", m->minRequestAge)
            .add("existingMounts", existingMounts)
            .add("maxDrivesAllowed", m->maxDrivesAllowed);
      lc.log(log::DEBUG, "In Scheduler::sortAndGetTapesForMountInfo(): Removing potential mount not passing criteria");
      m = mountInfo->potentialMounts.erase(m);
    } else {
      // populate the mount with a weight 
      m->ratioOfMountQuotaUsed = 1.0L * existingMounts / m->maxDrivesAllowed;
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
      if ( m->type == common::dataStructures::MountType::Retrieve) {
        params.add("tapeVid", m->vid);
      }
      params.add("mountType", common::dataStructures::toString(m->type))
            .add("existingMounts", existingMounts)
            .add("bytesQueued", m->bytesQueued)
            .add("minBytesToWarrantMount", m_minBytesToWarrantAMount)
            .add("filesQueued", m->filesQueued)
            .add("minFilesToWarrantMount", m_minFilesToWarrantAMount)
            .add("oldestJobAge", time(NULL) - m->oldestJobStartTime)
            .add("minArchiveRequestAge", m->minRequestAge)
            .add("existingMounts", existingMounts)
            .add("maxDrivesAllowed", m->maxDrivesAllowed)
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
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfoNoLock(lc);
  getMountInfoTime = timer.secs(utils::Timer::resetCounter);
  ExistingMountSummary existingMountsSummary;
  std::set<std::string> tapesInUse;
  std::list<catalogue::TapeForWriting> tapeList;
  
  sortAndGetTapesForMountInfo(mountInfo, logicalLibraryName, driveName, timer,
      existingMountsSummary, tapesInUse, tapeList,
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
          uint32_t existingMounts = 0;
          try {
            existingMounts=existingMountsSummary.at(TapePoolMountPair(m->tapePool, common::dataStructures::getMountBasicType(m->type))).totalMounts;
          } catch (...) {}
          log::ScopedParamContainer params(lc);
          params.add("tapePool", m->tapePool)
                .add("tapeVid", t.vid)
                .add("mountType", common::dataStructures::toString(m->type))
                .add("existingMounts", existingMounts)
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
          lc.log(log::DEBUG, "In Scheduler::getNextMountDryRun(): Found a potential mount (archive)");
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
      uint32_t existingMounts = 0;
      try {
        existingMounts=existingMountsSummary.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
      } catch (...) {}
      schedulerDbTime = getMountInfoTime;
      catalogueTime = getTapeInfoTime + getTapeForWriteTime;
      params.add("tapePool", m->tapePool)
            .add("tapeVid", m->vid)
            .add("mountType", common::dataStructures::toString(m->type))
            .add("existingMounts", existingMounts);
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
      lc.log(log::DEBUG, "In Scheduler::getNextMountDryRun(): Found a potential mount (retrieve)");
      return true;
    }
  }
  schedulerDbTime = getMountInfoTime;
  catalogueTime = getTapeInfoTime + getTapeForWriteTime;
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
  double catalogueTime = 0;
  std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> mountInfo;
  mountInfo = m_db.getMountInfo(lc);
  getMountInfoTime = timer.secs(utils::Timer::resetCounter);
  if (mountInfo->queueTrimRequired) {
    m_db.trimEmptyQueues(lc);
    queueTrimingTime = timer.secs(utils::Timer::resetCounter);
  }
  __attribute__((unused)) SchedulerDatabase::TapeMountDecisionInfo & debugMountInfo = *mountInfo;
  
  ExistingMountSummary existingMountsSummary;
  std::set<std::string> tapesInUse;
  std::list<catalogue::TapeForWriting> tapeList;
  
  sortAndGetTapesForMountInfo(mountInfo, logicalLibraryName, driveName, timer,
      existingMountsSummary, tapesInUse, tapeList,
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
            uint32_t existingMounts = 0;
            try {
              existingMounts=existingMountsSummary.at(TapePoolMountPair(m->tapePool, common::dataStructures::getMountBasicType(m->type))).totalMounts;
            } catch (...) {}
            schedulerDbTime = getMountInfoTime + queueTrimingTime + mountCreationTime + driveStatusSetTime;
            catalogueTime = getTapeInfoTime + getTapeForWriteTime;
            
            params.add("tapePool", m->tapePool)
                  .add("tapeVid", t.vid)
                  .add("vo",t.vo)
                  .add("mediaType",t.mediaType)
                  .add("vendor",t.vendor)
                  .add("mountType", common::dataStructures::toString(m->type))
                  .add("existingMounts", existingMounts)
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
            lc.log(log::DEBUG, "In Scheduler::getNextMount(): Selected next mount (archive)");
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
        std::unique_ptr<RetrieveMount> internalRet (
          new RetrieveMount(mountInfo->createRetrieveMount(m->vid, 
            m->tapePool,
            driveName,
            logicalLibraryName, 
            utils::getShortHostname(), 
            m->vo,
            m->mediaType,
            m->vendor,
            m->capacityInBytes,
            time(NULL), actvityAndWeight)));
        mountCreationTime += timer.secs(utils::Timer::resetCounter);
        internalRet->m_sessionRunning = true;
        internalRet->m_diskRunning = true;
        internalRet->m_tapeRunning = true;
        driveStatusSetTime += timer.secs(utils::Timer::resetCounter);
        log::ScopedParamContainer params(lc);
        uint32_t existingMounts = 0;
        try {
          existingMounts=existingMountsSummary.at(TapePoolMountPair(m->tapePool, m->type)).totalMounts;
        } catch (...) {}
        schedulerDbTime = getMountInfoTime + queueTrimingTime + mountCreationTime + driveStatusSetTime;
        catalogueTime = getTapeInfoTime + getTapeForWriteTime;
        params.add("tapePool", m->tapePool)
              .add("tapeVid", m->vid)
              .add("vo",m->vo)
              .add("mediaType",m->mediaType)
              .add("vendor",m->vendor)
              .add("mountType", common::dataStructures::toString(m->type))
              .add("existingMounts", existingMounts);
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
        lc.log(log::DEBUG, "In Scheduler::getNextMount(): Selected next mount (retrieve)");
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
  catalogueTime = getTapeInfoTime + getTapeForWriteTime;
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
// getQueuesAndMountSummaries
//------------------------------------------------------------------------------
std::list<common::dataStructures::QueueAndMountSummary> Scheduler::getQueuesAndMountSummaries(log::LogContext& lc) {
  std::list<common::dataStructures::QueueAndMountSummary> ret;
  // Obtain a map of vids to tape info from the catalogue
  auto vid_to_tapeinfo = m_catalogue.getAllTapes();

  // Extract relevant information from the object store.
  utils::Timer t;
  auto mountDecisionInfo=m_db.getMountInfoNoLock(lc);
  auto schedulerDbTime = t.secs(utils::Timer::resetCounter);
  auto & mdi __attribute__((unused)) = *mountDecisionInfo;

  for (auto & pm: mountDecisionInfo->potentialMounts) {
    // Find or create the relevant entry.
    auto &summary = common::dataStructures::QueueAndMountSummary::getOrCreateEntry(ret, pm.type, pm.tapePool, pm.vid, vid_to_tapeinfo);
    switch (pm.type) {
    case common::dataStructures::MountType::ArchiveForUser:
    case common::dataStructures::MountType::ArchiveForRepack:
      summary.mountPolicy.archivePriority = pm.priority;
      summary.mountPolicy.archiveMinRequestAge = pm.minRequestAge;
      summary.mountPolicy.maxDrivesAllowed = pm.maxDrivesAllowed;
      summary.bytesQueued = pm.bytesQueued;
      summary.filesQueued = pm.filesQueued;
      summary.oldestJobAge = time(nullptr) - pm.oldestJobStartTime ;
      break;
    case common::dataStructures::MountType::Retrieve:
      // TODO: we should remove the retrieveMinRequestAge if it's redundant, or rename pm.minArchiveRequestAge.
      summary.mountPolicy.retrieveMinRequestAge = pm.minRequestAge;
      summary.mountPolicy.retrievePriority = pm.priority;
      summary.mountPolicy.maxDrivesAllowed = pm.maxDrivesAllowed;
      summary.bytesQueued = pm.bytesQueued;
      summary.filesQueued = pm.filesQueued;
      summary.oldestJobAge = time(nullptr) - pm.oldestJobStartTime ;
      break;
    default:
      break;
    }
  }
  for (auto & em: mountDecisionInfo->existingOrNextMounts) {
    auto &summary = common::dataStructures::QueueAndMountSummary::getOrCreateEntry(ret, em.type, em.tapePool, em.vid, vid_to_tapeinfo);
    switch (em.type) {
    case common::dataStructures::MountType::ArchiveForUser:
    case common::dataStructures::MountType::ArchiveForRepack:
    case common::dataStructures::MountType::Retrieve:
      if (em.currentMount) 
        summary.currentMounts++;
      else
        summary.nextMounts++;
      summary.currentBytes += em.bytesTransferred;
      summary.currentFiles += em.filesTransferred;
      summary.latestBandwidth += em.latestBandwidth;
      break;
    default:
      break;
    }
  }
  mountDecisionInfo.reset();
  // Add the tape information where useful (archive queues).
  for (auto & mountOrQueue: ret) {
    if (common::dataStructures::MountType::ArchiveForUser==mountOrQueue.mountType || common::dataStructures::MountType::ArchiveForRepack==mountOrQueue.mountType) {
      // Get all the tape for this pool
      cta::catalogue::TapeSearchCriteria tsc;
      tsc.tapePool = mountOrQueue.tapePool;
      auto tapes=m_catalogue.getTapes(tsc);
      for (auto & t:tapes) {
        mountOrQueue.tapesCapacity += t.capacityInBytes;
        mountOrQueue.filesOnTapes += t.lastFSeq;
        mountOrQueue.dataOnTapes += t.dataOnTapeInBytes;
        if (!t.dataOnTapeInBytes)
          mountOrQueue.emptyTapes++;
        if (t.disabled) mountOrQueue.disabledTapes++;
        if (t.full) mountOrQueue.fullTapes++;
        if (t.readOnly) mountOrQueue.readOnlyTapes++;
        if (!t.full && !t.disabled && !t.readOnly) mountOrQueue.writableTapes++;
      }
    } else if (common::dataStructures::MountType::Retrieve==mountOrQueue.mountType) {
      // Get info for this tape.
      cta::catalogue::TapeSearchCriteria tsc;
      tsc.vid = mountOrQueue.vid;
      auto tapes=m_catalogue.getTapes(tsc);
      if (tapes.size() != 1) {
        throw cta::exception::Exception("In Scheduler::getQueuesAndMountSummaries(): got unexpected number of tapes from catalogue for a retrieve.");
      }
      auto &t=tapes.front();
      mountOrQueue.tapesCapacity += t.capacityInBytes;
      mountOrQueue.filesOnTapes += t.lastFSeq;
      mountOrQueue.dataOnTapes += t.dataOnTapeInBytes;
      if (!t.dataOnTapeInBytes)
        mountOrQueue.emptyTapes++;
      if (t.disabled) mountOrQueue.disabledTapes++;
      if (t.full) mountOrQueue.fullTapes++;
      if (t.readOnly) mountOrQueue.readOnlyTapes++;
      if (!t.full && !t.disabled && !t.readOnly) mountOrQueue.writableTapes++;
      mountOrQueue.tapePool = t.tapePoolName;
    }
  }
  auto respondPreparationTime = t.secs();
  log::ScopedParamContainer spc(lc);
  spc.add("schedulerDbTime", schedulerDbTime)
     .add("respondPreparationTime", respondPreparationTime);
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
      j->reportFailed(ex.getMessageValue(), lc);
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
      current.archiveJob->reportFailed(ex.getMessageValue(), lc);
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

} // namespace cta
