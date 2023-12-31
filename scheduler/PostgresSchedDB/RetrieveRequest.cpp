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

#include "scheduler/PostgresSchedDB/RetrieveRequest.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"

// generated during build in the build tree
#include "PostgresSchedDB/rowblobs.pb.h"

namespace cta::postgresscheddb {

void RetrieveRequest::insert() {
  postgresscheddb::sql::RetrieveJobQueueRow           row;
  postgresscheddb::blobser::RetrieveJobs              rj;
  postgresscheddb::blobser::RetrieveRequestRepackInfo ri;

  row.retrieveReqId = m_requestId;
  row.mountId       = m_mountId;
  row.status        = m_status;
  row.vid           = m_vid;
  row.priority      = m_priority;
  row.retMinReqAge  = m_retrieveMinReqAge;
  row.startTime     = m_startTime;
  row.failureReportUrl = m_failureReportUrl;
  row.failureReportLog = m_failureReportLog;
  row.isFailed         = m_isFailed;

  row.retrieveRequest = m_schedRetrieveReq;
  row.archiveFile     = m_archiveFile;

  // the tapeFiles from the archiveFile are not stored in the row
  // Instead each tapefile is a part of the retrieve job protobuf.
  // So fill in the protobuf Job and Tapefile info here:

  for(auto &j: m_jobs) {
    postgresscheddb::blobser::RetrieveJob *pb_job = rj.add_jobs();
    postgresscheddb::blobser::TapeFile *pb_tf = pb_job->mutable_tapefile();
    const cta::common::dataStructures::TapeFile *tf = 0;
    for(auto &f: m_archiveFile.tapeFiles) {
      if (f.copyNb == j.copyNb) {
        tf = &f;
        break;
      }
    }
    if (!tf) {
      throw std::runtime_error("Found job without tapefile");
    }
    pb_tf->set_vid(tf->vid);
    pb_tf->set_fseq(tf->fSeq);
    pb_tf->set_blockid(tf->blockId);
    pb_tf->set_filesize(tf->fileSize);
    pb_tf->set_copynb(tf->copyNb);
    pb_tf->set_creationtime(tf->creationTime);
    pb_tf->set_checksumblob(tf->checksumBlob.serialize());

    pb_job->set_copynb(j.copyNb);
    pb_job->set_maxtotalretries(j.maxtotalretries);
    pb_job->set_maxretrieswithinmount(j.maxretrieswithinmount);
    pb_job->set_retrieswithinmount(j.retrieswithinmount);
    pb_job->set_totalretries(j.totalretries);
    pb_job->set_lastmountwithfailure(j.lastmountwithfailure);
    pb_job->set_maxreportretries(j.maxreportretries);
    pb_job->set_totalreportretries(j.totalreportretries);
    pb_job->set_isfailed(j.isfailed);

    for(auto &s: j.failurelogs) {
      pb_job->add_failurelogs(s);
    }

    for(auto &s: j.reportfailurelogs) {
      pb_job->add_reportfailurelogs(s);
    }

    switch(j.status) {
      case postgresscheddb::RetrieveJobStatus::RJS_ToTransfer:
        pb_job->set_status(postgresscheddb::blobser::RetrieveJobStatus::RJS_ToTransfer);
        break;
      case postgresscheddb::RetrieveJobStatus::RJS_ToReportToUserForFailure:
        pb_job->set_status(postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToUserForFailure);
        break;
      case postgresscheddb::RetrieveJobStatus::RJS_Failed:
        pb_job->set_status(postgresscheddb::blobser::RetrieveJobStatus::RJS_Failed);
        break;
      case postgresscheddb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess:
        pb_job->set_status(postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
        break;
      case postgresscheddb::RetrieveJobStatus::RJS_ToReportToRepackForFailure:
        pb_job->set_status(postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
        break;
      default:
        throw  std::runtime_error("unexpected status in RetrieveRequest insert");
        break;
    }
  }

  row.mountPolicyName= m_mountPolicyName;

  row.activity       = m_activity;
  row.diskSystemName = m_diskSystemName;
  row.actCopyNb      = m_actCopyNb;

  // isrepack & repackReqId are stored both individually in the row and inside
  // the repackinfo protobuf
  row.isRepack = m_repackInfo.isRepack;
  row.repackReqId = m_repackInfo.repackRequestId;

  ri.set_fseq(m_repackInfo.fSeq);
  ri.set_file_buffer_url(m_repackInfo.fileBufferURL);
  ri.set_has_user_provided_file(m_repackInfo.hasUserProvidedFile);

  for(auto &m: m_repackInfo.archiveRouteMap) {
    postgresscheddb::blobser::RetrieveRequestArchiveRoute *ar = ri.add_archive_routes();
    ar->set_copynb(m.first);
    ar->set_tapepool(m.second);
  }

  for(auto &c: m_repackInfo.copyNbsToRearchive) {
    ri.add_copy_nbs_to_rearchive(c);
  }

  rj.SerializeToString(&row.retrieveJobsProtoBuf);
  ri.SerializeToString(&row.repackInfoProtoBuf);

  log::ScopedParamContainer params(m_lc);
  row.addParamsToLogContext(params);

  m_txn.reset(new postgresscheddb::Transaction(*m_connPool));

  try {
    row.insert(*m_txn);
  } catch(exception::Exception &ex) {
    params.add("exeptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In RetrieveRequest::insert(): failed to queue job.");
    throw;
  }

  m_lc.log(log::INFO, "In RetrieveRequest::insert(): added job to queue.");
}

void RetrieveRequest::update() {
  throw std::runtime_error("update not implemented.");
}

void RetrieveRequest::commit() {
  if (m_txn) {
    m_txn->commit();
  }
  m_txn.reset();
}

void RetrieveRequest::setFailureReason(const std::string & reason) {
  throw std::runtime_error("setFailureReason not implemented.");
}

bool RetrieveRequest::addJobFailure(uint32_t copyNumber, uint64_t mountId, const std::string & failureReason, log::LogContext & lc) {
  throw std::runtime_error("addJobFailure not implemented.");
}

void RetrieveRequest::setRepackInfo(const cta::postgresscheddb::RetrieveRequest::RetrieveReqRepackInfo & repackInfo) {
  throw std::runtime_error("setRepackInfo not implemented.");
}

void RetrieveRequest::setJobStatus(uint32_t copyNumber, const cta::postgresscheddb::RetrieveJobStatus &status) {
  throw std::runtime_error("setJobStatus not implemented.");
}

void RetrieveRequest::setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest & retrieveRequest) {
  m_schedRetrieveReq = retrieveRequest;
}

void RetrieveRequest::setRetrieveFileQueueCriteria(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {

  m_archiveFile = criteria.archiveFile;
  m_mountPolicyName = criteria.mountPolicy.name;

  m_priority = criteria.mountPolicy.retrievePriority;
  m_retrieveMinReqAge = criteria.mountPolicy.retrieveMinRequestAge;

  const uint32_t hardcodedRetriesWithinMount = m_repackInfo.isRepack ? 1 : 3;
  const uint32_t hardcodedTotalRetries       = m_repackInfo.isRepack ? 1 : 6;
  const uint32_t hardcodedReportRetries      = 2;

  m_jobs.clear();

  // create jobs for each tapefile in the archiveFile;
  for(auto &tf: m_archiveFile.tapeFiles) {
    m_jobs.emplace_back();
    m_jobs.back().copyNb = tf.copyNb;
    m_jobs.back().maxretrieswithinmount = hardcodedRetriesWithinMount;
    m_jobs.back().maxtotalretries = hardcodedTotalRetries;
    m_jobs.back().maxreportretries = hardcodedReportRetries;
  }
}

void RetrieveRequest::setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest & retrieveRequest,
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria) {
  m_activity = retrieveRequest.activity;
}

void RetrieveRequest::setDiskSystemName(const std::string & diskSystemName) {
  m_diskSystemName = diskSystemName;
}

void RetrieveRequest::setCreationTime(const uint64_t creationTime) {
  m_schedRetrieveReq.lifecycleTimings.creation_time = creationTime;
}

void RetrieveRequest::setFirstSelectedTime(const uint64_t firstSelectedTime) {
  throw std::runtime_error("setFirstSelectedTime not implemented.");
}

void RetrieveRequest::setCompletedTime(const uint64_t completedTime) {
  throw std::runtime_error("setCompletedTime not implemented.");
}

void RetrieveRequest::setReportedTime(const uint64_t reportedTime) {
  throw std::runtime_error("setReportedTime not implemented.");
}

void RetrieveRequest::setActiveCopyNumber(uint32_t activeCopyNb) {
  m_actCopyNb = activeCopyNb;

  // copy the active job info to the request level columns
  for(auto &j: m_jobs) {
    if (j.copyNb != m_actCopyNb) continue;
    for(auto &tf: m_archiveFile.tapeFiles) {
      if (tf.copyNb != m_actCopyNb) continue;

      m_status = j.status;
      m_vid = tf.vid;
    }
  }

}

void RetrieveRequest::setIsVerifyOnly(bool isVerifyOnly) {
  m_schedRetrieveReq.isVerifyOnly = isVerifyOnly;
}

void RetrieveRequest::setFailed() {
  throw std::runtime_error("setFailed not implemented.");
}

std::list<RetrieveRequest::RetrieveReqJobDump> RetrieveRequest::dumpJobs() {
  throw std::runtime_error("dumpJobs not implemented.");
}

RetrieveRequest& RetrieveRequest::operator=(const postgresscheddb::sql::RetrieveJobQueueRow &row) {
  postgresscheddb::blobser::RetrieveJobs rj;
  postgresscheddb::blobser::RetrieveRequestRepackInfo ri;

  rj.ParseFromString(row.retrieveJobsProtoBuf);
  ri.ParseFromString(row.repackInfoProtoBuf);

  m_requestId         = row.retrieveReqId;
  m_mountId           = row.mountId;
  m_status            = row.status;
  m_vid               = row.vid;
  m_priority          = row.priority;
  m_retrieveMinReqAge = row.retMinReqAge;
  m_startTime         = row.startTime;
  m_failureReportUrl  = row.failureReportUrl;
  m_failureReportLog  = row.failureReportLog;
  m_isFailed          = row.isFailed;

  m_schedRetrieveReq = row.retrieveRequest;
  m_archiveFile      = row.archiveFile;

  // the archiveFile above doesn't include the tapeFiles list. We only consider
  // tapefiles that the scheduler originally gave us for in the criteria of
  // the retrieve request (which might be a subset of those in the catalogue
  // for the given archiveFile). The tape files are packed inside the jobs list
  // in the row.

  for(auto &j: rj.jobs()) {
    m_archiveFile.tapeFiles.emplace_back();
    m_archiveFile.tapeFiles.back().vid          = j.tapefile().vid();
    m_archiveFile.tapeFiles.back().fSeq         = j.tapefile().fseq();
    m_archiveFile.tapeFiles.back().blockId      = j.tapefile().blockid();
    m_archiveFile.tapeFiles.back().fileSize     = j.tapefile().filesize();
    m_archiveFile.tapeFiles.back().copyNb       = j.tapefile().copynb();
    m_archiveFile.tapeFiles.back().creationTime = j.tapefile().creationtime();
    m_archiveFile.tapeFiles.back().checksumBlob.deserialize( j.tapefile().checksumblob() );
  }

  m_mountPolicyName  = row.mountPolicyName;

  m_activity         = row.activity;
  m_diskSystemName   = row.diskSystemName;
  m_actCopyNb        = row.actCopyNb;

  m_repackInfo.isRepack        = row.isRepack;
  m_repackInfo.repackRequestId = row.repackReqId;
  m_repackInfo.archiveRouteMap.clear();
  for(auto &rm: ri.archive_routes()) {
    m_repackInfo.archiveRouteMap[rm.copynb()] = rm.tapepool();
  }
  m_repackInfo.copyNbsToRearchive.clear();
  for(auto &cn: ri.copy_nbs_to_rearchive()) {
    m_repackInfo.copyNbsToRearchive.insert(cn);
  }
  m_repackInfo.fSeq                = ri.fseq();
  m_repackInfo.fileBufferURL       = ri.file_buffer_url();
  m_repackInfo.hasUserProvidedFile = ri.has_user_provided_file();

  m_jobs.clear();
  for(auto &j: rj.jobs()) {
    m_jobs.emplace_back();
    m_jobs.back().copyNb                = j.copynb();
    m_jobs.back().maxtotalretries       = j.maxtotalretries();
    m_jobs.back().maxretrieswithinmount = j.maxretrieswithinmount();
    m_jobs.back().retrieswithinmount    = j.retrieswithinmount();
    m_jobs.back().totalretries          = j.totalretries();
    m_jobs.back().lastmountwithfailure  = j.lastmountwithfailure();
    for(auto &fl: j.failurelogs()) {
      m_jobs.back().failurelogs.push_back(fl);
    }
    m_jobs.back().maxreportretries   = j.maxreportretries();
    m_jobs.back().totalreportretries = j.totalreportretries();
    for(auto &rfl: j.reportfailurelogs()) {
      m_jobs.back().reportfailurelogs.push_back(rfl);
    }
    m_jobs.back().isfailed = j.isfailed();
    switch(j.status()) {
      case postgresscheddb::blobser::RetrieveJobStatus::RJS_ToTransfer:
        m_jobs.back().status = postgresscheddb::RetrieveJobStatus::RJS_ToTransfer;
        break;
      case postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToUserForFailure:
        m_jobs.back().status = postgresscheddb::RetrieveJobStatus::RJS_ToReportToUserForFailure;
        break;
      case postgresscheddb::blobser::RetrieveJobStatus::RJS_Failed:
        m_jobs.back().status = postgresscheddb::RetrieveJobStatus::RJS_Failed;
        break;
      case postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToRepackForSuccess:
        m_jobs.back().status = postgresscheddb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess;
        break;
      case postgresscheddb::blobser::RetrieveJobStatus::RJS_ToReportToRepackForFailure:
        m_jobs.back().status = postgresscheddb::RetrieveJobStatus::RJS_ToReportToRepackForFailure;
        break;
      default:
        throw std::runtime_error("unexpected status in RetrieveRequest assign from row");
        break;
    }
  }

  return *this;
}

} // namespace cta::postgresscheddb
