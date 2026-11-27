/**
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RetrieveRequest.hpp"

#include "common/exception/NotImplementedException.hpp"

namespace cta::schedulerdb {

std::unique_ptr<postgres::RetrieveJobQueueRow> RetrieveRequest::makeJobRow() const {
  auto rjr = std::make_unique<postgres::RetrieveJobQueueRow>();
  // bogus retrieveRequestId, it will be given automatically by the insert
  // to avoid separate trip to the DB like cta::schedulerdb::postgres::RetrieveJobQueueRow::getNextRetrieveRequestID(m_conn);
  rjr->retrieveRequestId = 0;
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
  rjr->creationTime = m_schedRetrieveReq.creationLog.time;
  rjr->activity = m_activity;

  rjr->copyNb = m_actCopyNb;
  // The status is RetrieveJobStatus::RJS_ToTransfer, except for NO RECALL repack jobs
  // for which the status will be directly reporting the job to get transformed to archival job.
  rjr->status = m_status;
  rjr->vid = m_vid;
  rjr->fSeq = m_fSeq;
  rjr->blockId = m_blockId;
  rjr->mountPolicy = m_mountPolicyName;
  // Tape Pool is used only for repack retrieve to ease the transition
  // to the archival stage as we have this information at the start of repack already !
  auto it = m_repackInfo.archiveRouteMap.find(m_actCopyNb);
  if (it != m_repackInfo.archiveRouteMap.end()) {
    rjr->tapePool = it->second;
  }
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

  // Retry limits for the retrieve itself, independent of m_jobs: m_jobs holds one entry per
  // archive copy this subrequest will create (built from copy numbers in copyNbsToRearchive, see
  // initialize()), which for a pure AddCopiesOnly repack is never the recalled copy, and can
  // legitimately be empty (every missing copy is itself an archive destination with no existing
  // tape file to iterate - see initialize()'s own loop). The retrieve still needs a real retry
  // budget regardless of whether m_jobs has anything in it - setting these only inside the loop
  // below left an empty-m_jobs row at RetrieveJobQueueRow's default of 0, so a single transient
  // recall failure (totalRetries 0 -> 1 >= maxTotalRetries 0) exceeded retries immediately, and
  // that same 0 limit was then carried into the archive jobs this subrequest's retrieve feeds.
  rjr->maxRetriesWithinMount = m_repackInfo.isRepack ? RETRIES_WITHIN_MOUNT_FOR_REPACK : RETRIES_WITHIN_MOUNT_FOR_USER;
  rjr->maxTotalRetries = m_repackInfo.isRepack ? TOTAL_RETRIES_FOR_REPACK : TOTAL_RETRIES_FOR_USER;
  rjr->maxReportRetries = REPORT_RETRIES;

  // Populate alternates from jobs
  int i = 0;
  for (const auto& rj : m_jobs) {
    rjr->alternateVids += rj.vid + ",";
    rjr->alternateCopyNbs += std::to_string(rj.copyNb) + ",";
    rjr->alternateFSeq += std::to_string(rj.fSeq) + ",";
    rjr->alternateBlockId += std::to_string(rj.blockId) + ",";

    if (i++ == 0) {
      // Overrides the defaults above with the first job's own values - identical values in
      // practice (initialize() derives a job's limits from the same isRepack flag), kept to leave
      // the non-empty-m_jobs case's exact behaviour untouched.
      rjr->retriesWithinMount = rj.retriesWithinMount;
      rjr->maxRetriesWithinMount = rj.maxRetriesWithinMount;
      rjr->maxReportRetries = rj.maxReportRetries;
      rjr->totalRetries = rj.totalRetries;
      rjr->totalReportRetries = rj.totalReportRetries;
      rjr->lastMountWithFailure = rj.lastMountWithFailure;
      rjr->maxTotalRetries = rj.maxTotalRetries;
    }
  }

  if (!rjr->alternateVids.empty()) {
    rjr->alternateVids.pop_back();
  }
  if (!rjr->alternateCopyNbs.empty()) {
    rjr->alternateCopyNbs.pop_back();
  }
  if (!rjr->alternateFSeq.empty()) {
    rjr->alternateFSeq.pop_back();
  }
  if (!rjr->alternateBlockId.empty()) {
    rjr->alternateBlockId.pop_back();
  }

  // For any additional copies that will need to be archive for repack,
  // we need to record the additional requested copyNbs
  std::string copyNbsToRearchiveString = "";
  std::string tapePoolsForReparchiveCopyNbsString = "";
  for (const auto& copyNb : m_repackInfo.copyNbsToRearchive) {
    if (copyNb == 0) {
      continue;
    }
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
  if (!copyNbsToRearchiveString.empty()) {
    rjr->rearchiveCopyNbs = copyNbsToRearchiveString;
  }
  if (!tapePoolsForReparchiveCopyNbsString.empty()) {
    rjr->rearchiveTapePools = tapePoolsForReparchiveCopyNbsString;
  }
  return rjr;
}

// Inserts one row into DB
void RetrieveRequest::insert() {
  try {
    auto row = makeJobRow();
    log::ScopedParamContainer params(m_lc);
    row->addParamsToLogContext(params);
    row->insert(m_conn);
    m_conn.setRowCountForTelemetry(1);
    m_lc.log(log::INFO, "In RetrieveRequest::insert(): added jobs to queue.");
    m_conn.commit();
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer(m_lc).add(semconv::log::exceptionMessage, ex.getMessageValue());
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
      m_jobs.back().maxRetriesWithinMount =
        m_repackInfo.isRepack ? RETRIES_WITHIN_MOUNT_FOR_REPACK : RETRIES_WITHIN_MOUNT_FOR_USER;
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

  // Find the active job - may legitimately not exist. For a pure AddCopiesOnly repack
  // (Scheduler::expandRepackRequest()), the active copy number names the EXISTING tape copy being
  // retrieved FROM, which is deliberately excluded from copyNbsToRearchive - and therefore from
  // m_jobs, see initialize() - because that copy is being read, not rearchived. A Job represents
  // an archive DESTINATION, not the retrieve source; the two only happen to coincide for
  // MoveOnly/MoveAndAddCopies, where the active copy is also one being rewritten.
  for (const auto& j : m_jobs) {
    if (j.copyNb == activeCopyNb) {
      activeJob = &j;
      break;
    }
  }

  // Find the active tape file - this one is NOT optional: the active copy number was chosen from
  // this same archive file's own existing tape copies (see
  // RepackRequest::addSubrequestsAndUpdateStats()/RelationalDB::queueRetrieve()), so a missing
  // tape file here means that choice and the catalogue's view of the file have gone inconsistent.
  for (const auto& tf : m_archiveFile.tapeFiles) {
    if (tf.copyNb == activeCopyNb) {
      activeTapeFile = &tf;
      break;
    }
  }

  if (!activeTapeFile) {
    throw RetrieveRequestException("Active tape file not found for the given copy number.");
  }

  // fSeq/blockId/vid always come from the tape file itself - the thing actually being read - not
  // from the job: when a job exists for this copy number, it carries identical values anyway,
  // since initialize() sets a job's fSeq/blockId directly from this same tape file.
  m_fSeq = activeTapeFile->fSeq;
  m_blockId = activeTapeFile->blockId;
  m_vid = activeTapeFile->vid;
  // m_status is left at its current value when there is no job for the active copy
  // (AddCopiesOnly): the repack caller sets it explicitly right afterward via setJobStatus() (see
  // RepackRequest::addSubrequestsAndUpdateStats()), and every non-repack job already shares the
  // same RJS_ToTransfer default set in initialize(), regardless of which copy is "active".
  if (activeJob) {
    m_status = activeJob->status;
  }

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
