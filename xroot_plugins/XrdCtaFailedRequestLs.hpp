/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Failed Request Ls stream implementation
 * @copyright      Copyright 2019 CERN
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

#pragma once

#include <XrdSsiPbOStreamBuffer.hpp>
#include <scheduler/Scheduler.hpp>
#include <scheduler/OStoreDB/OStoreDB.hpp>



namespace cta { namespace xrd {

/*!
 * Stream object which implements "failedrequest ls" command.
 */
class FailedRequestLsStream : public XrdSsiStream
{
public:
  FailedRequestLsStream(Scheduler &scheduler, OStoreDB::ArchiveQueueItor_t archiveQueueItor,
    OStoreDB::RetrieveQueueItor_t retrieveQueueItor, bool is_log_entries, bool is_summary,
    log::LogContext &lc) :
      XrdSsiStream(XrdSsiStream::isActive),
      m_scheduler(scheduler),
      m_archiveQueueItor(std::move(archiveQueueItor)),
      m_retrieveQueueItor(std::move(retrieveQueueItor)),
      m_isLogEntries(is_log_entries),
      m_isSummary(is_summary),
      m_lc(lc)
  {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "FailedRequestLsStream() constructor");
  }

  virtual ~FailedRequestLsStream() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~FailedRequestLsStream() destructor");
  }

  /*!
   * Synchronously obtain data from an active stream
   *
   * Active streams can only exist on the server-side. This XRootD SSI Stream class is marked as an
   * active stream in the constructor.
   *
   * @param[out]       eInfo   The object to receive any error description.
   * @param[in,out]    dlen    input:  the optimal amount of data wanted (this is a hint)
   *                           output: the actual amount of data returned in the buffer.
   * @param[in,out]    last    input:  should be set to false.
   *                           output: if true it indicates that no more data remains to be returned
   *                                   either for this call or on the next call.
   *
   * @return    Pointer to the Buffer object that contains a pointer to the the data (see below). The
   *            buffer must be returned to the stream using Buffer::Recycle(). The next member is usable.
   * @retval    0    No more data remains or an error occurred:
   *                 last = true:  No more data remains.
   *                 last = false: A fatal error occurred, eRef has the reason.
   */
  virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) override {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): XrdSsi buffer fill request (", dlen, " bytes)");

    XrdSsiPb::OStreamBuffer<Data> *streambuf;

    try {
      if(m_isSummary) {
        // Special handling for --summary option
        streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);
        GetBuffSummary(streambuf);
        last = true;
      } else if(!m_archiveQueueItor.end()) {
        // List failed archive requests
        streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

        for(bool is_buffer_full = false; !m_archiveQueueItor.end() && !is_buffer_full; ++m_archiveQueueItor) {
          is_buffer_full = pushRecord(streambuf, m_archiveQueueItor.qid(), *m_archiveQueueItor);
        }
      } else if(!m_retrieveQueueItor.end()) {
        // List failed retrieve requests
        streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

        for(bool is_buffer_full = false; !m_retrieveQueueItor.end() && !is_buffer_full; ++m_retrieveQueueItor) {
          is_buffer_full = pushRecord(streambuf, m_retrieveQueueItor.qid(), *m_retrieveQueueItor);
        }
      } else {
        // Nothing more to send, close the stream
        last = true;
        return nullptr;
      }
      dlen = streambuf->Size();
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
      return streambuf;
    } catch(exception::Exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught CTA exception: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
    } catch(std::exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
    } catch(...) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
    }
    delete streambuf;
    return nullptr;
  }

  /*!
   * Populate the failed queue summary
   *
   * @param[in]    streambuf    XRootD SSI stream object to push records to
   */
  void GetBuffSummary(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
    SchedulerDatabase::JobsFailedSummary archive_summary;
    SchedulerDatabase::JobsFailedSummary retrieve_summary;

    bool isArchive  = !m_archiveQueueItor.end();
    bool isRetrieve = !m_retrieveQueueItor.end();

    if(isArchive) {
      Data record;
      archive_summary = m_scheduler.getArchiveJobsFailedSummary(m_lc);
      record.mutable_frls_summary()->set_request_type(admin::RequestType::ARCHIVE_REQUEST);
      record.mutable_frls_summary()->set_total_files(archive_summary.totalFiles);
      record.mutable_frls_summary()->set_total_size(archive_summary.totalBytes);
      streambuf->Push(record);
    }
    if(isRetrieve) {
      Data record;
      retrieve_summary = m_scheduler.getRetrieveJobsFailedSummary(m_lc);
      record.mutable_frls_summary()->set_request_type(admin::RequestType::RETRIEVE_REQUEST);
      record.mutable_frls_summary()->set_total_files(retrieve_summary.totalFiles);
      record.mutable_frls_summary()->set_total_size(retrieve_summary.totalBytes);
      streambuf->Push(record);
    }
    if(isArchive && isRetrieve) {
      Data record;
      record.mutable_frls_summary()->set_request_type(admin::RequestType::TOTAL);
      record.mutable_frls_summary()->set_total_files(archive_summary.totalFiles + retrieve_summary.totalFiles);
      record.mutable_frls_summary()->set_total_size(archive_summary.totalBytes + retrieve_summary.totalBytes);
      streambuf->Push(record);
    }

    m_isSummary = false;
  }

  /*!
   * Add a record to the stream
   *
   * @param[in]    streambuf      XRootD SSI stream object to push records to
   * @param[in]    requestType    The type of failed request (archive or retrieve)
   *
   * @retval    true     Stream buffer is full and ready to send
   * @retval    false    Stream buffer is not full
   */
  template<typename QueueType>
  bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf, const std::string &qid, const QueueType &item);

private:
  /*!
   * Map queue type to RequestType
   *
   * @return RequestType for the template specialisation
   */
  template<typename QueueType>
  admin::RequestType getRequestType(const QueueType &item);

  Scheduler &m_scheduler;                               //!< Reference to CTA Scheduler
  OStoreDB::ArchiveQueueItor_t  m_archiveQueueItor;     //!< Archive Queue Iterator
  OStoreDB::RetrieveQueueItor_t m_retrieveQueueItor;    //!< Retrieve Queue Iterator
  bool m_isLogEntries;                                  //!< Show failure log messages (verbose)
  bool m_isSummary;                                     //!< Show only summary of items in the failed queues
  log::LogContext &m_lc;                                //!< Reference to CTA Log Context

  static constexpr const char* const LOG_SUFFIX  = "FailedRequestLsStream";    //!< Identifier for SSI log messages
};



template<typename QueueType>
bool FailedRequestLsStream::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf, const std::string &qid, const QueueType &item)
{
  Data record;

  record.mutable_frls_item()->set_request_type(getRequestType(item));
#if 0
  switch(requestType) {
    case admin::RequestType::ARCHIVE_REQUEST:
      record.mutable_frls_item()->set_tapepool("tapepool"); break;
    case admin::RequestType::RETRIEVE_REQUEST:
      record.mutable_frls_item()->mutable_tf()->set_vid("vid"); break;
    default:
      throw exception::Exception("Unrecognised RequestType: " + std::to_string(requestType));
  }
  record.mutable_frls_item()->set_copy_nb(1);
  record.mutable_frls_item()->mutable_requester()->set_username("u");
  record.mutable_frls_item()->mutable_requester()->set_groupname("g");
  record.mutable_frls_item()->mutable_af()->mutable_df()->set_path("/path/");
  //record.mutable_frls()->set_failurelogs(...
#endif
  return streambuf->Push(record);
}



template<>
admin::RequestType FailedRequestLsStream::getRequestType(const common::dataStructures::ArchiveJob &item) { return admin::RequestType::ARCHIVE_REQUEST; }

template<>
admin::RequestType FailedRequestLsStream::getRequestType(const common::dataStructures::RetrieveJob &item) { return admin::RequestType::RETRIEVE_REQUEST; }

}} // namespace cta::xrd
