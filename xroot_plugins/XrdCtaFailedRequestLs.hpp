/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>

#include "xroot_plugins/XrdCtaStream.hpp"
#include "common/dataStructures/JobQueueType.hpp"

namespace cta { namespace xrd {

/*!
 * Stream object which implements "failedrequest ls" command.
 */
class FailedRequestLsStream : public XrdCtaStream {
 public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   * @param[in]    schedDb       CTA ObjectStore
   * @param[in]    lc            CTA Log Context
   */
  FailedRequestLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, SchedulerDatabase &schedDb, log::LogContext &lc);

 private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_isSummary ? m_isSummaryDone
                       : !isArchiveJobs() && !isRetrieveJobs();
  }

  bool isArchiveJobs() const {
    return m_archiveQueueItorPtr && !m_archiveQueueItorPtr->end();
  }

  bool isRetrieveJobs() const {
    return m_retrieveQueueItorPtr && !m_retrieveQueueItorPtr->end();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  /*!
   * Add a record to the stream
   *
   * @param[in]    streambuf      XRootD SSI stream object to push records to
   * @param[in]    item           Archive or Retrieve Job (used for template specialisation)
   *
   * @retval    true     Stream buffer is full and ready to send
   * @retval    false    Stream buffer is not full
   */
  template<typename QueueType>
  bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf, const QueueType &item);

  /*!
   * Populate the failed queue summary
   *
   * @param[in]    streambuf         XRootD SSI stream object to push records to
   */
  void GetBuffSummary(XrdSsiPb::OStreamBuffer<Data> *streambuf);


  std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor>  m_archiveQueueItorPtr;     //!< Archive Queue Iterator
  std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> m_retrieveQueueItorPtr;    //!< Retrieve Queue Iterator
  bool m_isSummary;                                                         //!< Show only summary of items in the failed queues
  bool m_isSummaryDone;                                                     //!< Summary has been sent
  bool m_isLogEntries;                                                      //!< Show failure log messages (verbose)
  log::LogContext &m_lc;                                                    //!< Reference to CTA Log Context

  static constexpr const char* const LOG_SUFFIX  = "FailedRequestLsStream";  //!< Identifier for SSI log messages
};


FailedRequestLsStream::FailedRequestLsStream(const frontend::AdminCmdStream& requestMsg,
  cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, SchedulerDatabase &schedDb,
  log::LogContext &lc) :
    XrdCtaStream(catalogue, scheduler),
    m_isSummary(requestMsg.has_flag(admin::OptionBoolean::SUMMARY)),
    m_isSummaryDone(false),
    m_isLogEntries(requestMsg.has_flag(admin::OptionBoolean::SHOW_LOG_ENTRIES)),
    m_lc(lc) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "FailedRequestLsStream() constructor");

  if (m_isLogEntries && m_isSummary) {
    throw cta::exception::UserError("--log and --summary are mutually exclusive");
  }

  auto tapepool     = requestMsg.getOptional(cta::admin::OptionString::TAPE_POOL);
  auto vid          = requestMsg.getOptional(cta::admin::OptionString::VID);
  bool justarchive  = requestMsg.has_flag(cta::admin::OptionBoolean::JUSTARCHIVE)  || tapepool;
  bool justretrieve = requestMsg.has_flag(cta::admin::OptionBoolean::JUSTRETRIEVE) || vid;

  if (justarchive && justretrieve) {
    throw cta::exception::UserError("--justarchive/--tapepool and --justretrieve/--vid options are mutually exclusive");
  }

  using common::dataStructures::JobQueueType;
  if (!justretrieve)
    m_archiveQueueItorPtr = schedDb.getArchiveJobQueueItor(tapepool ? *tapepool : "", JobQueueType::FailedJobs);
  if (!justarchive)
    m_retrieveQueueItorPtr = schedDb.getRetrieveJobQueueItor(vid ? *vid : "", JobQueueType::FailedJobs);
}

/*!
 * pushRecord ArchiveJob specialisation
 */
template<> bool FailedRequestLsStream::
pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf, const common::dataStructures::ArchiveJob &item)
{
  auto &tapepool = m_archiveQueueItorPtr->qid();
  Data record;

  record.mutable_frls_item()->set_object_id(item.objectId);
  record.mutable_frls_item()->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
  record.mutable_frls_item()->set_tapepool(tapepool);
  record.mutable_frls_item()->set_copy_nb(item.copyNumber);
  record.mutable_frls_item()->mutable_requester()->set_username(item.request.requester.name);
  record.mutable_frls_item()->mutable_requester()->set_groupname(item.request.requester.group);
  record.mutable_frls_item()->mutable_af()->set_archive_id(item.archiveFileID);
  record.mutable_frls_item()->mutable_af()->set_disk_instance(item.instanceName);
  record.mutable_frls_item()->mutable_af()->set_disk_id(item.request.diskFileID);
  record.mutable_frls_item()->mutable_af()->set_size(item.request.fileSize);
  record.mutable_frls_item()->mutable_af()->set_storage_class(item.request.storageClass);
  record.mutable_frls_item()->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
  record.mutable_frls_item()->mutable_af()->set_creation_time(item.request.creationLog.time);
  record.mutable_frls_item()->set_totalretries(item.totalRetries);
  record.mutable_frls_item()->set_totalreportretries(item.totalReportRetries);
  if (m_isLogEntries) {
    *record.mutable_frls_item()->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
    *record.mutable_frls_item()->mutable_reportfailurelogs() = { item.reportfailurelogs.begin(), item.reportfailurelogs.end() };
  }
  return streambuf->Push(record);
}


/*!
 * pushRecord RetrieveJob specialisation
 */
template<> bool FailedRequestLsStream::
pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf, const common::dataStructures::RetrieveJob &item)
{
  auto &vid = m_retrieveQueueItorPtr->qid();

  Data record;

  record.mutable_frls_item()->set_object_id(item.objectId);
  record.mutable_frls_item()->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
  record.mutable_frls_item()->set_copy_nb(item.tapeCopies.at(vid).first);
  record.mutable_frls_item()->mutable_requester()->set_username(item.request.requester.name);
  record.mutable_frls_item()->mutable_requester()->set_groupname(item.request.requester.group);
  record.mutable_frls_item()->mutable_af()->set_archive_id(item.request.archiveFileID);
  record.mutable_frls_item()->mutable_af()->set_size(item.fileSize);
  record.mutable_frls_item()->mutable_af()->mutable_df()->set_path(item.request.diskFileInfo.path);
  record.mutable_frls_item()->mutable_af()->set_creation_time(item.request.creationLog.time);
  record.mutable_frls_item()->mutable_tf()->set_vid(vid);
  record.mutable_frls_item()->set_totalretries(item.totalRetries);
  record.mutable_frls_item()->set_totalreportretries(item.totalReportRetries);

  // Find the correct tape copy
  for (auto &tapecopy : item.tapeCopies) {
    auto &tf = tapecopy.second.second;
    if (tf.vid == vid) {
      record.mutable_frls_item()->mutable_tf()->set_f_seq(tf.fSeq);
      record.mutable_frls_item()->mutable_tf()->set_block_id(tf.blockId);
      break;
    }
  }

  if (m_isLogEntries) {
    *record.mutable_frls_item()->mutable_failurelogs() = { item.failurelogs.begin(), item.failurelogs.end() };
    *record.mutable_frls_item()->mutable_reportfailurelogs() = {item.reportfailurelogs.begin(), item.reportfailurelogs.end()};
  }
  return streambuf->Push(record);
}


int FailedRequestLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  if (m_isSummary) {
    // Special handling for -S (Summary) option
    GetBuffSummary(streambuf);
  } else {
    // List failed archive requests
    if (isArchiveJobs()) {
      for (bool is_buffer_full = false; !m_archiveQueueItorPtr->end() && !is_buffer_full; ++*m_archiveQueueItorPtr) {
        is_buffer_full = pushRecord(streambuf, **m_archiveQueueItorPtr);
      }
    }
    // List failed retrieve requests
    if (isRetrieveJobs()) {
      for (bool is_buffer_full = false; !m_retrieveQueueItorPtr->end() && !is_buffer_full; ++*m_retrieveQueueItorPtr) {
        is_buffer_full = pushRecord(streambuf, **m_retrieveQueueItorPtr);
      }
    }
  }
  return streambuf->Size();
}



void FailedRequestLsStream::GetBuffSummary(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  SchedulerDatabase::JobsFailedSummary archive_summary;
  SchedulerDatabase::JobsFailedSummary retrieve_summary;

  if (isArchiveJobs()) {
    Data record;
    archive_summary = m_scheduler.getArchiveJobsFailedSummary(m_lc);
    record.mutable_frls_summary()->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
    record.mutable_frls_summary()->set_total_files(archive_summary.totalFiles);
    record.mutable_frls_summary()->set_total_size(archive_summary.totalBytes);
    streambuf->Push(record);
  }
  if (isRetrieveJobs()) {
    Data record;
    retrieve_summary = m_scheduler.getRetrieveJobsFailedSummary(m_lc);
    record.mutable_frls_summary()->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
    record.mutable_frls_summary()->set_total_files(retrieve_summary.totalFiles);
    record.mutable_frls_summary()->set_total_size(retrieve_summary.totalBytes);
    streambuf->Push(record);
  }
  if (isArchiveJobs() && isRetrieveJobs()) {
    Data record;
    record.mutable_frls_summary()->set_request_type(admin::RequestType::TOTAL);
    record.mutable_frls_summary()->set_total_files(archive_summary.totalFiles + retrieve_summary.totalFiles);
    record.mutable_frls_summary()->set_total_size(archive_summary.totalBytes + retrieve_summary.totalBytes);
    streambuf->Push(record);
  }

  m_isSummaryDone = true;
}

}  // namespace xrd
}  // namespace cta
