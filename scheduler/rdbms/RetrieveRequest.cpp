/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2023 CERN
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

#include "scheduler/rdbms/RetrieveRequest.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

std::unique_ptr<postgres::RetrieveJobQueueRow>
RetrieveRequest::makeJobRow() const {
  auto rjr = std::make_unique<postgres::RetrieveJobQueueRow>();
  rjr->retrieveRequestId = cta::schedulerdb::postgres::RetrieveJobQueueRow::getNextRetrieveRequestID(m_conn);
  rjr->repackRequestId = m_repackInfo.repackRequestId;
  rjr->reqJobCount = m_jobs.size();

  // rdbms request members
  rjr->diskSystemName = m_diskSystemName;
  rjr->requesterName = m_schedRetrieveReq.requester.name;
  rjr->requesterGroup = m_schedRetrieveReq.requester.group;
  rjr->dstURL = m_schedRetrieveReq.dstURL;
  rjr->retrieveReportURL = m_schedRetrieveReq.retrieveReportURL;
  rjr->retrieveErrorReportURL = m_schedRetrieveReq.errorReportURL;
  rjr->isVerifyOnly = m_isVerifyOnly;
  rjr->srrUsername = m_schedRetrieveReq.creationLog.username;
  rjr->srrHost = m_schedRetrieveReq.creationLog.host;
  rjr->srrTime = m_schedRetrieveReq.creationLog.time;
  rjr->lifecycleTimings_creation_time = m_schedRetrieveReq.lifecycleTimings.creation_time;
  rjr->lifecycleTimings_first_selected_time = m_schedRetrieveReq.lifecycleTimings.first_selected_time;
  rjr->lifecycleTimings_completed_time = m_schedRetrieveReq.lifecycleTimings.completed_time;
  rjr->activity = m_activity;

  rjr->copyNb = m_actCopyNb;
  rjr->status = RetrieveJobStatus::RJS_ToTransfer;
  rjr->vid = m_vid;
  rjr->fSeq = m_fSeq;
  rjr->blockId = m_blockId;
  rjr->mountPolicy = m_mountPolicyName;
  rjr->priority = m_priority;
  rjr->minRetrieveRequestAge = m_retrieveMinReqAge;

  rjr->archiveFileID = m_archiveFile.archiveFileID;
  rjr->diskFileId = m_archiveFile.diskFileId;
  rjr->diskInstance = m_archiveFile.diskInstance;
  rjr->fileSize = m_archiveFile.fileSize;
  rjr->storageClass = m_archiveFile.storageClass;
  rjr->diskFileInfoPath = m_archiveFile.diskFileInfo.path;
  rjr->diskFileInfoOwnerUid = m_archiveFile.diskFileInfo.owner_uid;
  rjr->diskFileInfoGid = m_archiveFile.diskFileInfo.gid;
  rjr->checksumBlob = m_archiveFile.checksumBlob;
  rjr->startTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  // Populate alternates from jobs
  int i = 0;
  for (const auto &rj : m_jobs) {
    rjr->alternateVids     += rj.vid + ",";
    rjr->alternateCopyNbs  += std::to_string(rj.copyNb) + ",";
    rjr->alternateFSeq     += std::to_string(rj.fSeq) + ",";
    rjr->alternateBlockId  += std::to_string(rj.blockId) + ",";

    if (i++ == 0) {
      rjr->retriesWithinMount   = rj.retriesWithinMount;
      rjr->maxRetriesWithinMount = rj.maxRetriesWithinMount;
      rjr->maxReportRetries     = rj.maxReportRetries;
      rjr->totalRetries         = rj.totalRetries;
      rjr->totalReportRetries   = rj.totalReportRetries;
      rjr->lastMountWithFailure = rj.lastMountWithFailure;
      rjr->maxTotalRetries      = rj.maxTotalRetries;
    }
  }

  if (!rjr->alternateVids.empty())        rjr->alternateVids.pop_back();
  if (!rjr->alternateCopyNbs.empty())     rjr->alternateCopyNbs.pop_back();
  if (!rjr->alternateFSeq.empty())        rjr->alternateFSeq.pop_back();
  if (!rjr->alternateBlockId.empty())     rjr->alternateBlockId.pop_back();

  // For any additional copies that will need to be archive for repack,
  // we need to record the additional requested copyNbs
  std::string copyNbsToRearchiveString = "";
  std::string tapePoolsForReparchiveCopyNbsString = "";
  for (const auto& copyNb : m_repackInfo.copyNbsToRearchive) {
    if (copyNb == 0) { continue; }
    if (!copyNbsToRearchiveString.empty()) {
      copyNbsToRearchiveString += ",";
      tapePoolsForReparchiveCopyNbsString += ",";
    }
    copyNbsToRearchiveString += std::to_string(copyNb);
    tapePoolsForReparchiveCopyNbsString += m_repackInfo.archiveRouteMap.at(copyNb);
  }
  log::ScopedParamContainer(m_lc)
      .add("copyNbsToRearchiveString", copyNbsToRearchiveString)
      .add("tapePoolsForReparchiveCopyNbsString", tapePoolsForReparchiveCopyNbsString)
      .log(log::DEBUG, "In RetrieveRequest::insert(): copyNbsToRearchiveString check.");
  if (!copyNbsToRearchiveString.empty()){
    rjr->rearchiveCopyNbs = copyNbsToRearchiveString;
  }
  if (!tapePoolsForReparchiveCopyNbsString.empty()){
    rjr->rearchiveTapePools = tapePoolsForReparchiveCopyNbsString;
  }
  rjr->srrMountPolicy = "?";
  rjr->srrActivity    = m_schedRetrieveReq.activity.value_or("?");

  return rjr;
}

// Inserts one row into DB
void RetrieveRequest::insert() {
  try{
    auto row = makeJobRow();
    log::ScopedParamContainer params(m_lc);
    row->addParamsToLogContext(params);
    row->insert(m_conn);
    m_lc.log(log::INFO, "In RetrieveRequest::insert(): added jobs to queue.");
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer (lc).add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In RetrieveRequest::insert(): failed to queue job.");
    m_conn.rollback();  // Rollback on error
    throw;
  }
}

void RetrieveRequest::update() const {
  throw exception::NotImplementedException();
}

void RetrieveRequest::commit() {
  m_conn.commit();
}

[[noreturn]] void RetrieveRequest::setFailureReason([[maybe_unused]] std::string_view reason) const {
  throw exception::NotImplementedException();
}

[[noreturn]] bool RetrieveRequest::addJobFailure([[maybe_unused]] uint32_t copyNumber,
                                                 [[maybe_unused]] uint64_t mountId,
                                                 [[maybe_unused]] std::string_view failureReason) const {
  throw exception::NotImplementedException();
}

void RetrieveRequest::setRepackInfo(const cta::schedulerdb::RetrieveRequest::RetrieveReqRepackInfo& repackInfo) {
  m_repackInfo = repackInfo;
}

void RetrieveRequest::setJobStatus([[maybe_unused]] uint32_t copyNumber,
                                   const cta::schedulerdb::RetrieveJobStatus& status) {
  m_status = status;
}

void RetrieveRequest::setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest& retrieveRequest) {
  m_schedRetrieveReq = retrieveRequest;
}

void RetrieveRequest::fillJobsSetRetrieveFileQueueCriteria(
  const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  m_archiveFile = criteria.archiveFile;
  m_mountPolicyName = criteria.mountPolicy.name;

  m_priority = criteria.mountPolicy.retrievePriority;
  m_retrieveMinReqAge = criteria.mountPolicy.retrieveMinRequestAge;

  m_jobs.clear();

  // create jobs for each tapefile in the archiveFile;
  for (const auto& tf : m_archiveFile.tapeFiles) {
    if (!m_repackInfo.isRepack || (m_repackInfo.isRepack && m_repackInfo.copyNbsToRearchive.contains(tf.copyNb))) {
      m_jobs.emplace_back();
      m_jobs.back().copyNb = tf.copyNb;
      m_jobs.back().vid = tf.vid;
      m_jobs.back().maxRetriesWithinMount = m_repackInfo.isRepack ? RETRIES_WITHIN_MOUNT_FOR_REPACK : RETRIES_WITHIN_MOUNT_FOR_USER;
      m_jobs.back().maxTotalRetries = m_repackInfo.isRepack ? TOTAL_RETRIES_FOR_REPACK : TOTAL_RETRIES_FOR_USER;
      m_jobs.back().maxReportRetries = REPORT_RETRIES;
      m_jobs.back().status = schedulerdb::RetrieveJobStatus::RJS_ToTransfer;
      // in case we need these for retrieval we should save them in DB as well !
      m_jobs.back().fSeq = tf.fSeq;
      m_jobs.back().blockId = tf.blockId;
    }
  }
}

void RetrieveRequest::setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest& retrieveRequest,
                                          const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  m_activity = retrieveRequest.activity;
}

void RetrieveRequest::setDiskSystemName(std::optional<std::string> diskSystemName) {
  m_diskSystemName = diskSystemName;
}

void RetrieveRequest::setCreationTime(const uint64_t creationTime) {
  m_schedRetrieveReq.lifecycleTimings.creation_time = creationTime;
}

void RetrieveRequest::setFirstSelectedTime(const uint64_t firstSelectedTime) const {
  throw exception::NotImplementedException();
}

void RetrieveRequest::setCompletedTime(const uint64_t completedTime) const {
  throw exception::NotImplementedException();
}

void RetrieveRequest::setReportedTime(const uint64_t reportedTime) const {
  throw exception::NotImplementedException();
}

void RetrieveRequest::setActiveCopyNumber(uint32_t activeCopyNb) {
  m_actCopyNb = activeCopyNb;

  const RetrieveRequest::Job* activeJob = nullptr;
  const cta::common::dataStructures::TapeFile* activeTapeFile = nullptr;

  // Find the active job
  for (const auto& j : m_jobs) {
    if (j.copyNb == activeCopyNb) {
      activeJob = &j;
      m_fSeq = j.fSeq;
      m_blockId = j.blockId;
      break;
    }
  }

  // Find the active tape file
  for (const auto& tf : m_archiveFile.tapeFiles) {
    if (tf.copyNb == activeCopyNb) {
      activeTapeFile = &tf;
      break;
    }
  }

  if (!activeJob || !activeTapeFile) {
    throw RetrieveRequestException("Active job or tape file not found for the given copy number.");
  }
  m_status = activeJob->status;
  m_vid = activeTapeFile->vid;

  // Validate all jobs have corresponding tape files
  for (const auto& j : m_jobs) {
    bool hasTapeFile = false;
    for (const auto& tf : m_archiveFile.tapeFiles) {
      if (tf.copyNb == j.copyNb) {
        hasTapeFile = true;
        break;  // Found the match, stop checking further
      }
    }
    if (!hasTapeFile) {
      throw RetrieveRequestException("Found job without corresponding tape file.");
    }
  }
}

void RetrieveRequest::setIsVerifyOnly(bool isVerifyOnly) {
  m_schedRetrieveReq.isVerifyOnly = isVerifyOnly;
}

void RetrieveRequest::setFailed() const {
  throw exception::NotImplementedException();
}

std::list<RetrieveRequest::JobDump> RetrieveRequest::dumpJobs() const {
  throw exception::NotImplementedException();
}
}  // namespace cta::schedulerdb
