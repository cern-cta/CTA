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
  m_conn = std::make_unique<cta::rdbms::Conn>(m_schedDb.getConn());

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

  if (m_isSummary) {
    collectSummaryData(!justretrieve, !justarchive);
  } else {
#ifdef CTA_PGSCHED
    if (!justretrieve) {
      try {
        auto archiveRows = m_schedDb.getArchiveJobRows(*m_conn, common::dataStructures::QueueType::Failed, tapepool);
        m_archiveJobQueueItorPtr = std::make_unique<rdbms::Rset>(std::move(archiveRows));
        if (m_archiveJobQueueItorPtr) {
          m_archiveHasNext = m_archiveJobQueueItorPtr->next();
        }
      } catch (exception::Exception& ex) {
        throw cta::exception::Exception("Unable to retrieve failed jobs from the backend: " + ex.getMessageValue());
      }
    }
    if (!justarchive) {
      try {
        auto retrieveRows = m_schedDb.getRetrieveJobRows(*m_conn, common::dataStructures::QueueType::Failed, vid);
        m_retrieveJobQueueItorPtr = std::make_unique<rdbms::Rset>(std::move(retrieveRows));
        if (m_retrieveJobQueueItorPtr) {
          m_retrieveHasNext = m_retrieveJobQueueItorPtr->next();
        }
      } catch (exception::Exception& ex) {
        throw cta::exception::Exception("Unable to retrieve failed jobs from the backend: " + ex.getMessageValue());
      }
    }
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

void FailedRequestLsResponseStream::fillCommonFields(cta::admin::FailedRequestLsItem& fr_item,
                                                     const cta::rdbms::Rset& item) {
  fr_item.set_copy_nb(item.columnUint64("COPY_NB"));

  fr_item.mutable_requester()->set_username(item.columnString("REQUESTER_NAME"));
  fr_item.mutable_requester()->set_groupname(item.columnString("REQUESTER_GROUP"));

  fr_item.mutable_af()->set_archive_id(item.columnUint64("ARCHIVE_FILE_ID"));
  fr_item.mutable_af()->set_disk_instance(item.columnString("DISK_INSTANCE"));
  fr_item.mutable_af()->set_disk_id(item.columnString("DISK_FILE_ID"));
  fr_item.mutable_af()->set_size(item.columnUint64("SIZE_IN_BYTES"));
  fr_item.mutable_af()->set_storage_class(item.columnString("STORAGE_CLASS"));
  fr_item.mutable_af()->mutable_df()->set_path(item.columnString("DISK_FILE_PATH"));
  fr_item.mutable_af()->set_creation_time(item.columnUint64("CREATION_TIME"));

  fr_item.set_totalretries(item.columnUint64("TOTAL_RETRIES"));
  fr_item.set_totalreportretries(item.columnUint64("TOTAL_REPORT_RETRIES"));

  if (m_isLogEntries) {
    fr_item.add_reportfailurelogs(item.columnString("REPORT_FAILURE_LOG"));
    fr_item.add_failurelogs(item.columnString("FAILURE_LOG"));
  }

  fr_item.set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
  fr_item.set_instance_name(m_instanceName);
}

cta::xrd::Data FailedRequestLsResponseStream::getNextArchiveJobsData() {
  const auto& item = *m_archiveJobQueueItorPtr;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  // Fill common fields
  fillCommonFields(*fr_item, item);

  // Fill archive specific fields
  fr_item->set_request_type(cta::admin::RequestType::ARCHIVE_REQUEST);
  fr_item->set_object_id(std::string("a:") + std::to_string(item.columnUint64("JOB_ID")));
  fr_item->set_tapepool(item.columnString("TAPE_POOL"));

  return data;
}

cta::xrd::Data FailedRequestLsResponseStream::getNextRetrieveJobsData() {
  const auto& item = *m_retrieveJobQueueItorPtr;

  cta::xrd::Data data;
  auto fr_item = data.mutable_frls_item();

  // Fill common fields
  fillCommonFields(*fr_item, item);

  //  Fill retrieve specific fields
  fr_item->set_request_type(cta::admin::RequestType::RETRIEVE_REQUEST);
  fr_item->set_object_id(std::string("r:") + std::to_string(item.columnUint64("JOB_ID")));
  fr_item->mutable_tf()->set_vid(item.columnString("VID"));
  fr_item->mutable_tf()->set_f_seq(item.columnUint64("FSEQ"));
  fr_item->mutable_tf()->set_block_id(item.columnUint64("BLOCK_ID"));

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
  const auto archiveDone = !m_archiveJobQueueItorPtr || !m_archiveHasNext;
  const auto retrieveDone = !m_retrieveJobQueueItorPtr || !m_retrieveHasNext;
#else
  const auto archiveDone = !m_archiveJobQueueItorPtr || m_archiveJobQueueItorPtr->end();
  const auto retrieveDone = !m_retrieveJobQueueItorPtr || m_retrieveJobQueueItorPtr->end();
#endif
  return archiveDone && retrieveDone;
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
  if (m_archiveJobQueueItorPtr && m_archiveHasNext) {
    // read the prepared row
    cta::xrd::Data data = getNextArchiveJobsData();
    // move to next row
    m_archiveHasNext = m_archiveJobQueueItorPtr->next();
    return data;
  }
  if (m_retrieveJobQueueItorPtr && m_retrieveHasNext) {
    // read the prepared row
    cta::xrd::Data data = getNextRetrieveJobsData();
    // move to next row
    m_retrieveHasNext = m_retrieveJobQueueItorPtr->next();
    return data;
  }
  throw std::runtime_error("Stream is exhausted");
#else
  if (m_archiveJobQueueItorPtr && !m_archiveJobQueueItorPtr->end()) {
    return getNextArchiveJobsData();
  } else {
    return getNextRetrieveJobsData();
  }
#endif
}

}  // namespace cta::frontend
