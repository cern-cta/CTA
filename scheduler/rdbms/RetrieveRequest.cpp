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

namespace cta::schedulerdb {

void RetrieveRequest::insert() {
  m_lc.log(log::INFO, "In RetrieveRequest::insert(): getting request ID from DB.");
  uint64_t rreq_id = cta::schedulerdb::postgres::RetrieveJobQueueRow::getNextRetrieveRequestID(m_conn);
  uint32_t rreq_job_count = m_jobs.size();
  m_lc.log(log::INFO, "In RetrieveRequest::insert(): creating jobs.");

  // Inserting the jobs to the DB
  try {
    cta::schedulerdb::postgres::RetrieveJobQueueRow rjr;
    rjr.retrieveRequestId = rreq_id;
    rjr.reqJobCount = rreq_job_count;

    // rdbms request members
    rjr.diskSystemName = m_diskSystemName;
    // m_schedRetrieveReq metadata fields
    rjr.requesterName = m_schedRetrieveReq.requester.name;
    rjr.requesterGroup = m_schedRetrieveReq.requester.group;
    rjr.dstURL = m_schedRetrieveReq.dstURL;
    rjr.retrieveReportURL = m_schedRetrieveReq.retrieveReportURL;
    rjr.retrieveErrorReportURL = m_schedRetrieveReq.errorReportURL;
    rjr.isVerifyOnly =
      m_isVerifyOnly;  // m_schedRetrieveReq.isVerifyOnly; // true = retrieve file from tape but do not write a disk copy
    rjr.srrUsername = m_schedRetrieveReq.creationLog.username;
    rjr.srrHost = m_schedRetrieveReq.creationLog.host;
    rjr.srrTime = m_schedRetrieveReq.creationLog.time;
    rjr.lifecycleTimings_creation_time = m_schedRetrieveReq.lifecycleTimings.creation_time;
    rjr.lifecycleTimings_first_selected_time = m_schedRetrieveReq.lifecycleTimings.first_selected_time;
    rjr.lifecycleTimings_completed_time = m_schedRetrieveReq.lifecycleTimings.completed_time;
    rjr.activity = m_activity;  // from m_schedRetrieveReq.activity; set if needed only

    /* Think about when to register this and when to put another vid found in queueRetrieve:
     * std::optional<std::string> m_schedRetrieveReq.vid; limit retrieve requests to the specified vid (in the case of dual-copy files)
     * std::optional<std::string> m_schedRetrieveReq.mountPolicy; limit retrieve requests to a specified mount policy (only used for verification requests)
     * std::optional<std::string> activity;
     * setActiveCopyNumber sets also vid and status;
     */
    rjr.copyNb = m_actCopyNb;
    rjr.status = RetrieveJobStatus::RJS_ToTransfer;  // m_status;
    rjr.vid = m_vid;
    rjr.fSeq = m_fSeq;
    rjr.blockId = m_blockId;
    // info from criteria passed to fillJobsSetRetrieveFileQueueCriteria()
    rjr.mountPolicy = m_mountPolicyName;
    rjr.priority = m_priority;
    rjr.minRetrieveRequestAge = m_retrieveMinReqAge;

    rjr.archiveFileID = m_archiveFile.archiveFileID;
    rjr.diskFileId = std::move(m_archiveFile.diskFileId);
    rjr.diskInstance = std::move(m_archiveFile.diskInstance);
    rjr.fileSize = m_archiveFile.fileSize;
    rjr.storageClass = std::move(m_archiveFile.storageClass);
    rjr.diskFileInfoPath = std::move(m_archiveFile.diskFileInfo.path);
    rjr.diskFileInfoOwnerUid = m_archiveFile.diskFileInfo.owner_uid;
    rjr.diskFileInfoGid = m_archiveFile.diskFileInfo.gid;
    rjr.checksumBlob = std::move(m_archiveFile.checksumBlob);
    rjr.startTime = time(nullptr);  // Time the job was queued in the DB

    // For each tape file concatenate the copyNb and vids into alternate strings to save for retrial/requeueing

    int i = 0;
    for (const auto& rj : m_jobs) {
      i++;
      rjr.alternateVids += rj.vid + std::string(",");
      rjr.alternateCopyNbs += std::to_string(rj.copyNb) + std::string(",");
      rjr.alternateFSeq += std::to_string(rj.fSeq) + std::string(",");
      rjr.alternateBlockId += std::to_string(rj.blockId) + std::string(",");

      if (i == 1) {
        rjr.retriesWithinMount = rj.retriesWithinMount;
        rjr.maxRetriesWithinMount = rj.maxRetriesWithinMount;
        rjr.maxReportRetries = rj.maxReportRetries;
        rjr.totalRetries = rj.totalRetries;
        rjr.totalReportRetries = rj.totalReportRetries;
        rjr.lastMountWithFailure = rj.lastMountWithFailure;
        rjr.maxTotalRetries = rj.maxTotalRetries;
      }
    }
    if (!rjr.alternateVids.empty()) {
      rjr.alternateVids.pop_back();
    }
    if (!rjr.alternateCopyNbs.empty()) {
      rjr.alternateCopyNbs.pop_back();
    }
    if (!rjr.alternateFSeq.empty()) {
      rjr.alternateFSeq.pop_back();
    }
    if (!rjr.alternateBlockId.empty()) {
      rjr.alternateBlockId.pop_back();
    }
    rjr.srrMountPolicy = "?";                                     // ? what was this for ?
    rjr.srrActivity = m_schedRetrieveReq.activity.value_or("?");  // ? what was this for ?
    log::ScopedParamContainer params(m_lc);
    rjr.addParamsToLogContext(params);
    m_lc.log(log::INFO, "In RetrieveRequest::insert(): before insert row.");
    rjr.insert(m_conn);
    m_lc.log(log::INFO, "In RetrieveRequest::insert(): added jobs to queue.");
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In RetrieveRequest::insert(): failed to queue job.");
    m_conn.rollback();  // Rollback on error
    throw;
  }
}

/* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
 * future rewrite using DB columns directly instead of inserting Protobuf objects
 *
 * ri.set_fseq(m_repackInfo.fSeq);
 * ri.set_file_buffer_url(m_repackInfo.fileBufferURL);
 * ri.set_has_user_provided_file(m_repackInfo.hasUserProvidedFile);
 * for(const auto &[key, value]: m_repackInfo.archiveRouteMap) {
 *   schedulerdb::blobser::RetrieveRequestArchiveRoute *ar = ri.add_archive_routes();
 *   ar->set_copynb(key);
 *   ar->set_tapepool(value);
 * }
 * for(auto &c: m_repackInfo.copyNbsToRearchive) {
 *   ri.add_copy_nbs_to_rearchive(c);
 * }
 * rj.SerializeToString(&row.retrieveJobsProtoBuf);
 * ri.SerializeToString(&row.repackInfoProtoBuf);
 */

void RetrieveRequest::update() const {
  throw RetrieveRequestException("update not implemented.");
}

void RetrieveRequest::commit() {
  m_conn.commit();
}

[[noreturn]] void RetrieveRequest::setFailureReason([[maybe_unused]] std::string_view reason) const {
  throw RetrieveRequestException("setFailureReason not implemented.");
}

[[noreturn]] bool RetrieveRequest::addJobFailure([[maybe_unused]] uint32_t copyNumber,
                                                 [[maybe_unused]] uint64_t mountId,
                                                 [[maybe_unused]] std::string_view failureReason) const {
  throw RetrieveRequestException("addJobFailure not implemented.");
}

void RetrieveRequest::setRepackInfo(const cta::schedulerdb::RetrieveRequest::RetrieveReqRepackInfo& repackInfo) const {
  throw RetrieveRequestException("setRepackInfo not implemented.");
}

void RetrieveRequest::setJobStatus(uint32_t copyNumber, const cta::schedulerdb::RetrieveJobStatus& status) const {
  throw RetrieveRequestException("setJobStatus not implemented.");
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

void RetrieveRequest::setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest& retrieveRequest,
                                          const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  m_activity = retrieveRequest.activity;
}

void RetrieveRequest::setDiskSystemName(std::string_view diskSystemName) {
  m_diskSystemName = diskSystemName;
}

void RetrieveRequest::setCreationTime(const uint64_t creationTime) {
  m_schedRetrieveReq.lifecycleTimings.creation_time = creationTime;
}

void RetrieveRequest::setFirstSelectedTime(const uint64_t firstSelectedTime) const {
  throw RetrieveRequestException("setFirstSelectedTime not implemented.");
}

void RetrieveRequest::setCompletedTime(const uint64_t completedTime) const {
  throw RetrieveRequestException("setCompletedTime not implemented.");
}

void RetrieveRequest::setReportedTime(const uint64_t reportedTime) const {
  throw RetrieveRequestException("setReportedTime not implemented.");
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
  throw RetrieveRequestException("setFailed not implemented.");
}

std::list<RetrieveRequest::JobDump> RetrieveRequest::dumpJobs() const {
  throw RetrieveRequestException("dumpJobs not implemented.");
}
}  // namespace cta::schedulerdb
