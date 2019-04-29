/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend List Pending Archive/List Pending Retrieves stream implementation
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

#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>


namespace cta { namespace xrd {

/*!
 * Stream object which implements "lpa" and "lpr" commands
 */
template<typename QueueItor_t>
class ListPendingQueueStream : public XrdCtaStream
{
public:
  ListPendingQueueStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, OStoreDB &schedDb);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_queueItor.end();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

private:
  bool m_isExtended;                                                      //!< Summary or extended listing?
  optional<std::string> m_filter;                                         //!< Tapepool or Vid to filter results

  QueueItor_t m_queueItor;                                                //!< Archive/Retrieve Queue Iterator
  typedef decltype(*m_queueItor) data_t;                                  //!< Infer data type from template type

  uint64_t fileSize(const data_t &job);                                   //!< Obtain file size from queue item
  bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,               //!< Convert data to protobufs and put on stream
    const std::string &tape_id, const data_t &job);
  bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,               //!< Convert summary to protobufs and put on stream
    const std::string &tape_id, const uint64_t &total_files,
    const uint64_t &total_size);

  static constexpr const char* const LOG_SUFFIX  = "ListPendingQueue";    //!< Identifier for log messages
};


template<>
ListPendingQueueStream<OStoreDB::ArchiveQueueItor_t>::
ListPendingQueueStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, OStoreDB &schedDb) :
  XrdCtaStream(catalogue, scheduler),
  m_isExtended(requestMsg.has_flag(admin::OptionBoolean::EXTENDED)),
  m_filter(requestMsg.getOptional(admin::OptionString::TAPE_POOL)),
  m_queueItor(schedDb.getArchiveJobItor(m_filter ? m_filter.value() : ""))
{
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ListPendingQueueStream<Archive>() constructor");
}


template<>
ListPendingQueueStream<OStoreDB::RetrieveQueueItor_t>::
ListPendingQueueStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, OStoreDB &schedDb) :
  XrdCtaStream(catalogue, scheduler),
  m_isExtended(requestMsg.has_flag(admin::OptionBoolean::EXTENDED)),
  m_filter(requestMsg.getOptional(admin::OptionString::VID)),
  m_queueItor(schedDb.getRetrieveJobItor(m_filter ? m_filter.value() : ""))
{
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ListPendingQueueStream<Retrieve>() constructor");
}


template <typename T>
int ListPendingQueueStream<T>::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf)
{
  // is_buffer_full is set to true when we have one full block of data in the buffer, i.e. enough data
  // to send to the client. The actual buffer size is double the block size, so we can keep writing a
  // few additional records after is_buffer_full is true. These will be sent on the next iteration. If
  // we exceed the hard limit of double the block size, Push() will throw an exception.

  if(m_isExtended) {
    // Detailed listing of all queued files
    for(bool is_buffer_full = false; !m_queueItor.end() && !is_buffer_full; ++m_queueItor) {
      is_buffer_full = pushRecord(streambuf, m_queueItor.qid(), *m_queueItor);
    }
  } else {
    // Summary by tapepool or vid

    for(bool is_buffer_full = false; !m_queueItor.end() && !is_buffer_full; )
    {
      uint64_t total_files = 0;
      uint64_t total_size = 0;

      auto qid = m_queueItor.qid();

      for(m_queueItor.beginq(); !m_queueItor.endq(); ++m_queueItor) {
        ++total_files;
        total_size += fileSize(*m_queueItor);
      }

      is_buffer_full = pushRecord(streambuf, qid, total_files, total_size);
    }
  }

  return streambuf->Size();
}


// Template specialisations for Archive Queue types

template<>
uint64_t ListPendingQueueStream<OStoreDB::ArchiveQueueItor_t>::fileSize(const data_t &job) {
  return job.request.fileSize;
}


template<>
bool ListPendingQueueStream<OStoreDB::ArchiveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
  const std::string &tapepool, const common::dataStructures::ArchiveJob &job)
{
  Data record;

  // Tapepool
  record.mutable_lpa_item()->set_tapepool(tapepool);

  // Copy number
  record.mutable_lpa_item()->set_copy_nb(job.copyNumber);

  // Archive file
  auto af = record.mutable_lpa_item()->mutable_af();
  af->set_archive_id(job.archiveFileID);
  af->set_disk_instance(job.instanceName);
  af->set_disk_id(job.request.diskFileID);
  af->set_size(job.request.fileSize);
  af->mutable_cs()->set_type(job.request.checksumType);
  af->mutable_cs()->set_value(job.request.checksumValue);         
  af->set_storage_class(job.request.storageClass);
  af->mutable_df()->mutable_owner_id()->set_uid(job.request.diskFileInfo.owner_uid);
  af->mutable_df()->mutable_owner_id()->set_gid(job.request.diskFileInfo.gid);
  af->mutable_df()->set_path(job.request.diskFileInfo.path);

  return streambuf->Push(record);
}


template<>
bool ListPendingQueueStream<OStoreDB::ArchiveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
  const std::string &tapepool, const uint64_t &total_files, const uint64_t &total_size)
{
  Data record;

  // Tapepool
  record.mutable_lpa_summary()->set_tapepool(tapepool);

  // Summary statistics
  record.mutable_lpa_summary()->set_total_files(total_files);
  record.mutable_lpa_summary()->set_total_size(total_size);

  return streambuf->Push(record);
}


// Template specialisations for Retrieve Queue types

template<>
uint64_t ListPendingQueueStream<OStoreDB::RetrieveQueueItor_t>::fileSize(const data_t &job) {
  return job.fileSize;
}


template<>
bool ListPendingQueueStream<OStoreDB::RetrieveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
  const std::string &vid, const common::dataStructures::RetrieveJob &job)
{
  bool is_buffer_full = false;

  // Write one record per tape copy

  for(auto tape_it = job.tapeCopies.begin(); tape_it != job.tapeCopies.end(); ++tape_it)
  {
    // If we are filtering by vid, skip the ones we are not interested in.
    //
    // Do we actually need to do this or are we guaranteed that the vid field is the same for all
    // objects in the same queue?
    if(!vid.empty() && vid != tape_it->first) continue;

    Data record;

    // Copy number
    record.mutable_lpr_item()->set_copy_nb(tape_it->second.first);

    // Archive file
    auto af = record.mutable_lpr_item()->mutable_af();
    af->set_archive_id(job.request.archiveFileID);
    af->set_size(job.fileSize);
    af->mutable_df()->mutable_owner_id()->set_uid(job.request.diskFileInfo.owner_uid);
    af->mutable_df()->mutable_owner_id()->set_gid(job.request.diskFileInfo.gid);
    af->mutable_df()->set_path(job.request.diskFileInfo.path);

    // Tape file
    auto tf = record.mutable_lpr_item()->mutable_tf();
    tf->set_vid(tape_it->first);
    tf->set_f_seq(tape_it->second.second.fSeq);
    tf->set_block_id(tape_it->second.second.blockId);

    is_buffer_full = streambuf->Push(record);
  }

  return is_buffer_full;
}


template<>
bool ListPendingQueueStream<OStoreDB::RetrieveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
  const std::string &vid, const uint64_t &total_files, const uint64_t &total_size)
{
  Data record;

  // Tapepool
  record.mutable_lpr_summary()->set_vid(vid);

  // Summary statistics
  record.mutable_lpr_summary()->set_total_files(total_files);
  record.mutable_lpr_summary()->set_total_size(total_size);

  return streambuf->Push(record);
}

}} // namespace cta::xrd
