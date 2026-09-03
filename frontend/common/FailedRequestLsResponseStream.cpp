/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FailedRequestLsResponseStream.hpp"

#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

namespace cta::frontend {

FailedRequestLsResponseStream::FailedRequestLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                             cta::Scheduler& scheduler,
                                                             const std::string& instanceName,
                                                             const admin::AdminCmd& adminCmd,
                                                             SchedulerDatabase& schedDb,
                                                             cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_schedDb(schedDb),
      m_lc(lc),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()) {
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(adminCmd);

  // Parse options
  m_isSummary = request.has_flag(OptionBoolean::SUMMARY);
  m_isLogEntries = request.has_flag(OptionBoolean::SHOW_LOG_ENTRIES);

  if (m_isLogEntries && m_isSummary) {
    throw cta::exception::UserError("--log and --summary are mutually exclusive");
  }

  auto tapepool = request.getOptional(OptionString::TAPE_POOL);
  auto vid = request.getOptional(OptionString::VID);
  bool justarchive = request.has_flag(OptionBoolean::JUSTARCHIVE) || tapepool.has_value();
  bool justretrieve = request.has_flag(OptionBoolean::JUSTRETRIEVE) || vid.has_value();

  if (justarchive && justretrieve) {
    throw cta::exception::UserError("--justarchive/--tapepool and --justretrieve/--vid options are mutually exclusive");
  }

  // The summary always covers all the failed jobs: the totals are not filtered by tape pool or
  // by VID, hence such a request is rejected rather than answered with misleading totals
  if (m_isSummary && (tapepool.has_value() || vid.has_value())) {
    throw cta::exception::UserError(
      "--summary reports the totals of all the failed jobs and is mutually exclusive with --tapepool and --vid");
  }

  if (m_isSummary) {
    collectSummaryData(!justretrieve, !justarchive);
  } else {
#ifdef CTA_PGSCHED
    // The summary queries use their own transaction from the pool, so a connection is only
    // needed to list the jobs. It must outlive the result sets fetched from it.
    m_conn = std::make_unique<cta::rdbms::Conn>(m_schedDb.getConn());

    m_tapePool = tapepool;
    m_vid = vid;

    // The user jobs and the repack jobs are kept in separate tables, hence one queue per table
    if (!justretrieve) {
      m_queuesToList.push_back(FailedQueue::UserArchive);
      m_queuesToList.push_back(FailedQueue::RepackArchive);
    }
    if (!justarchive) {
      m_queuesToList.push_back(FailedQueue::UserRetrieve);
      m_queuesToList.push_back(FailedQueue::RepackRetrieve);
    }
    // Open the first non-empty queue and position it on its first row
    advanceToNextRow();
#else
    if (!justretrieve) {
      m_archiveJobQueueItorPtr =
        m_schedDb.getArchiveJobQueueItor(tapepool ? *tapepool : "", common::dataStructures::JobQueueType::FailedJobs);
    }
    if (!justarchive) {
      m_retrieveJobQueueItorPtr =
        m_schedDb.getRetrieveJobQueueItor(vid ? *vid : "", common::dataStructures::JobQueueType::FailedJobs);
    }

#endif
  }
}

#ifdef CTA_PGSCHED

bool FailedRequestLsResponseStream::isArchiveQueue(FailedQueue failedQueue) {
  return failedQueue == FailedQueue::UserArchive || failedQueue == FailedQueue::RepackArchive;
}

bool FailedRequestLsResponseStream::isRepackQueue(FailedQueue failedQueue) {
  return failedQueue == FailedQueue::RepackArchive || failedQueue == FailedQueue::RepackRetrieve;
}

std::unique_ptr<rdbms::Rset> FailedRequestLsResponseStream::queryFailedQueue(FailedQueue failedQueue) {
  const bool repack = isRepackQueue(failedQueue);
  try {
    if (isArchiveQueue(failedQueue)) {
      return std::make_unique<rdbms::Rset>(
        m_schedDb.getArchiveJobRows(*m_conn, common::dataStructures::QueueType::Failed, m_tapePool, repack));
    }
    return std::make_unique<rdbms::Rset>(
      m_schedDb.getRetrieveJobRows(*m_conn, common::dataStructures::QueueType::Failed, m_vid, repack));
  } catch (exception::Exception& ex) {
    throw cta::exception::Exception("Unable to retrieve failed jobs from the backend: " + ex.getMessageValue());
  }
}

void FailedRequestLsResponseStream::advanceToNextRow() {
  while (!m_hasNext && !m_queuesToList.empty()) {
    m_currentQueue = m_queuesToList.front();
    m_queuesToList.pop_front();
    // Release the exhausted result set before querying the next queue, as the connection
    // can only have a single query in flight at a time
    m_currentRows.reset();
    m_currentRows = queryFailedQueue(m_currentQueue);
    // Position on the first row of the queue just opened. An empty queue leaves m_hasNext
    // false, so the loop moves on to the queue after it
    m_hasNext = m_currentRows->next();
  }
}

void FailedRequestLsResponseStream::fillCommonFields(cta::admin::FailedRequestLsItem& fr_item,
                                                     const cta::rdbms::Rset& item) {
  // Every column read here is nullable in the schema, and the repack jobs are created by the
  // system rather than by a user, so the requester and the disk file fields may well be unset
  // for them. The optional getters are used because columnString()/columnUint64() throw on a
  // null value, which would abort the whole listing instead of reporting an empty field.
  fr_item.set_copy_nb(item.columnOptionalUint64("COPY_NB").value_or(0));

  fr_item.mutable_requester()->set_username(item.columnOptionalString("REQUESTER_NAME").value_or(""));
  fr_item.mutable_requester()->set_groupname(item.columnOptionalString("REQUESTER_GROUP").value_or(""));

  fr_item.mutable_af()->set_archive_id(item.columnOptionalUint64("ARCHIVE_FILE_ID").value_or(0));
  fr_item.mutable_af()->set_disk_instance(item.columnOptionalString("DISK_INSTANCE").value_or(""));
  fr_item.mutable_af()->set_disk_id(item.columnOptionalString("DISK_FILE_ID").value_or(""));
  fr_item.mutable_af()->set_size(item.columnOptionalUint64("SIZE_IN_BYTES").value_or(0));
  fr_item.mutable_af()->set_storage_class(item.columnOptionalString("STORAGE_CLASS").value_or(""));
  fr_item.mutable_af()->mutable_df()->set_path(item.columnOptionalString("DISK_FILE_PATH").value_or(""));
  fr_item.mutable_af()->set_creation_time(item.columnOptionalUint64("CREATION_TIME").value_or(0));

  fr_item.set_totalretries(item.columnOptionalUint64("TOTAL_RETRIES").value_or(0));
  fr_item.set_totalreportretries(item.columnOptionalUint64("TOTAL_REPORT_RETRIES").value_or(0));

  if (m_isLogEntries) {
    fr_item.add_reportfailurelogs(item.columnOptionalString("REPORT_FAILURE_LOG").value_or(""));
    fr_item.add_failurelogs(item.columnOptionalString("FAILURE_LOG").value_or(""));
  }

  fr_item.set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
  fr_item.set_instance_name(m_instanceName);
}

cta::xrd::Data FailedRequestLsResponseStream::getNextArchiveJobsData() {
  const auto& item = *m_currentRows;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  // Fill common fields
  fillCommonFields(*fr_item, item);

  // Fill archive specific fields
  fr_item->set_request_type(cta::admin::RequestType::ARCHIVE_REQUEST);
  // The repack jobs are reported with the "ra:" prefix and the user jobs with "a:", so that
  // "failedrequest rm" knows which of the two tables the job ID is to be deleted from
  if (isRepackQueue(m_currentQueue)) {
    fr_item->set_object_id(std::string("ra:") + std::to_string(item.columnUint64("JOB_ID")));
  } else {
    fr_item->set_object_id(std::string("a:") + std::to_string(item.columnUint64("JOB_ID")));
  }
  fr_item->set_tapepool(item.columnString("TAPE_POOL"));

  return data;
}

cta::xrd::Data FailedRequestLsResponseStream::getNextRetrieveJobsData() {
  const auto& item = *m_currentRows;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  // Fill common fields
  fillCommonFields(*fr_item, item);

  //  Fill retrieve specific fields
  fr_item->set_request_type(cta::admin::RequestType::RETRIEVE_REQUEST);
  // The repack jobs are reported with the "rr:" prefix and the user jobs with "r:", so that
  // "failedrequest rm" knows which of the two tables the job ID is to be deleted from
  if (isRepackQueue(m_currentQueue)) {
    fr_item->set_object_id(std::string("rr:") + std::to_string(item.columnUint64("JOB_ID")));
  } else {
    fr_item->set_object_id(std::string("r:") + std::to_string(item.columnUint64("JOB_ID")));
  }
  fr_item->mutable_tf()->set_vid(item.columnString("VID"));  // NOT NULL in the schema
  fr_item->mutable_tf()->set_f_seq(item.columnOptionalUint64("FSEQ").value_or(0));
  fr_item->mutable_tf()->set_block_id(item.columnOptionalUint64("BLOCK_ID").value_or(0));

  return data;
}
#else
cta::xrd::Data FailedRequestLsResponseStream::getNextArchiveJobsData() {
  auto& tapePoolName = m_archiveJobQueueItorPtr->qid();
  const auto& item = **m_archiveJobQueueItorPtr;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  fr_item->set_object_id(item.objectId);
  fr_item->set_request_type(cta::admin::RequestType::ARCHIVE_REQUEST);
  fr_item->set_tapepool(tapePoolName);
  fr_item->set_copy_nb(item.copyNumber);
  fr_item->mutable_requester()->set_username(item.request.requester.name);
  fr_item->mutable_requester()->set_groupname(item.request.requester.group);
  fr_item->mutable_af()->set_archive_id(item.archiveFileID);
  fr_item->mutable_af()->set_disk_instance(item.instanceName);
  fr_item->mutable_af()->set_disk_id(item.request.diskFileID);
  fr_item->mutable_af()->set_size(item.request.fileSize);
  fr_item->mutable_af()->set_storage_class(item.request.storageClass);
  fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
  fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
  fr_item->set_totalretries(item.totalRetries);
  fr_item->set_totalreportretries(item.totalReportRetries);
  if (m_isLogEntries) {
    *fr_item->mutable_failurelogs() = {item.failurelogs.begin(), item.failurelogs.end()};
    *fr_item->mutable_reportfailurelogs() = {item.reportfailurelogs.begin(), item.reportfailurelogs.end()};
  }
  fr_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
  fr_item->set_instance_name(m_instanceName);

  ++*m_archiveJobQueueItorPtr;

  return data;
}

cta::xrd::Data FailedRequestLsResponseStream::getNextRetrieveJobsData() {
  auto& vid = m_retrieveJobQueueItorPtr->qid();
  const auto& item = **m_retrieveJobQueueItorPtr;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  fr_item->set_object_id(item.objectId);
  fr_item->set_request_type(cta::admin::RequestType::RETRIEVE_REQUEST);
  fr_item->set_copy_nb(item.tapeCopies.at(vid).first);
  fr_item->mutable_requester()->set_username(item.request.requester.name);
  fr_item->mutable_requester()->set_groupname(item.request.requester.group);
  fr_item->mutable_af()->set_archive_id(item.request.archiveFileID);
  fr_item->mutable_af()->set_size(item.fileSize);
  fr_item->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
  fr_item->mutable_af()->set_creation_time(item.request.creationLog.time);
  fr_item->mutable_tf()->set_vid(vid);
  fr_item->set_totalretries(item.totalRetries);
  fr_item->set_totalreportretries(item.totalReportRetries);

  // Find the correct tape copy
  for (const auto& [tapecopyKey, tapecopyValue] : item.tapeCopies) {
    auto& tf = tapecopyValue.second;
    if (tf.vid == vid) {
      fr_item->mutable_tf()->set_f_seq(tf.fSeq);
      fr_item->mutable_tf()->set_block_id(tf.blockId);
      break;
    }
  }

  if (m_isLogEntries) {
    *fr_item->mutable_failurelogs() = {item.failurelogs.begin(), item.failurelogs.end()};
    *fr_item->mutable_reportfailurelogs() = {item.reportfailurelogs.begin(), item.reportfailurelogs.end()};
  }
  fr_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
  fr_item->set_instance_name(m_instanceName);

  ++*m_retrieveJobQueueItorPtr;

  return data;
}
#endif

void FailedRequestLsResponseStream::collectSummaryData(bool hasArchive, bool hasRetrieve) {
  SchedulerDatabase::JobsFailedSummary archive_summary;
  SchedulerDatabase::JobsFailedSummary retrieve_summary;

  if (hasArchive) {
    archive_summary = m_schedDb.getArchiveJobsFailedSummary(m_lc);

    cta::xrd::Data data;
    data.mutable_frls_summary()->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
    data.mutable_frls_summary()->set_total_files(archive_summary.totalFiles);
    data.mutable_frls_summary()->set_total_size(archive_summary.totalBytes);
    data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
    data.mutable_frls_summary()->set_instance_name(m_instanceName);

    m_summaryData.emplace_back(std::move(data));
  }

  if (hasRetrieve) {
    retrieve_summary = m_schedDb.getRetrieveJobsFailedSummary(m_lc);

    cta::xrd::Data data;
    data.mutable_frls_summary()->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
    data.mutable_frls_summary()->set_total_files(retrieve_summary.totalFiles);
    data.mutable_frls_summary()->set_total_size(retrieve_summary.totalBytes);
    data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
    data.mutable_frls_summary()->set_instance_name(m_instanceName);

    m_summaryData.emplace_back(std::move(data));
  }

  if (hasArchive && hasRetrieve) {
    cta::xrd::Data data;
    data.mutable_frls_summary()->set_request_type(admin::RequestType::TOTAL);
    data.mutable_frls_summary()->set_total_files(archive_summary.totalFiles + retrieve_summary.totalFiles);
    data.mutable_frls_summary()->set_total_size(archive_summary.totalBytes + retrieve_summary.totalBytes);
    data.mutable_frls_summary()->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
    data.mutable_frls_summary()->set_instance_name(m_instanceName);

    m_summaryData.emplace_back(std::move(data));
  }
}

bool FailedRequestLsResponseStream::isDone() {
  if (m_isSummary) {
    return m_summaryData.empty();
  }
#ifdef CTA_PGSCHED
  // advanceToNextRow() only leaves m_hasNext false once every queue has been reported
  return !m_hasNext;
#else
  const auto archiveDone = !m_archiveJobQueueItorPtr || m_archiveJobQueueItorPtr->end();
  const auto retrieveDone = !m_retrieveJobQueueItorPtr || m_retrieveJobQueueItorPtr->end();
  return archiveDone && retrieveDone;
#endif
}

cta::xrd::Data FailedRequestLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  if (m_isSummary) {
    cta::xrd::Data data = std::move(m_summaryData.front());
    m_summaryData.pop_front();
    return data;
  }
#ifdef CTA_PGSCHED
  // Read the prepared row of the queue being listed, before the cursor is moved on, so that
  // the object ID prefix matches the table this row was read from
  cta::xrd::Data data = isArchiveQueue(m_currentQueue) ? getNextArchiveJobsData() : getNextRetrieveJobsData();
  // Move to the next row of the current queue, then on to the following queue if it is exhausted
  m_hasNext = m_currentRows->next();
  advanceToNextRow();
  return data;
#else
  if (m_archiveJobQueueItorPtr && !m_archiveJobQueueItorPtr->end()) {
    return getNextArchiveJobsData();
  } else {
    return getNextRetrieveJobsData();
  }
#endif
}

}  // namespace cta::frontend
