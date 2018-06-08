/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend List Pending Files stream implementation
 * @copyright      Copyright 2017 CERN
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
#include <scheduler/OStoreDB/OStoreDB.hpp>



namespace cta { namespace xrd {

/*!
 * Stream object which implements "lpa" and "lpr" commands
 */
template<typename QueueItor_t>
class ListPendingQueue : public XrdSsiStream
{
public:
   ListPendingQueue(cta::catalogue::Catalogue &catalogue, bool is_extended, QueueItor_t queueItor) :
      XrdSsiStream(XrdSsiStream::isActive),
      m_catalogue(catalogue),
      m_isExtended(is_extended),
      m_queueItor(std::move(queueItor))
   {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ListPendingQueue() constructor");
   }

   virtual ~ListPendingQueue() {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~ListPendingQueue() destructor");
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
         // If there is nothing more to send, close the stream
         if(m_queueItor.end())
         {
            last = true;
            return nullptr;
         }

         // Fill the buffer

         streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

         /*
          * is_buffer_full is set to true when we have one full block of data in the buffer, i.e. enough
          * data to send to the client. The actual buffer size is double the block size, so we can keep
          * writing a few additional records after is_buffer_full is true. These will be sent on the next
          * iteration. If we exceed the hard limit of double the block size, Push() will throw an exception.  
          */

         if(m_isExtended) {
            // Detailed listing of all queued files

            for(bool is_buffer_full = false; !m_queueItor.end() && !is_buffer_full; ++m_queueItor)
            {
               is_buffer_full = pushRecord(streambuf, m_queueItor.qid(), *m_queueItor);
            }
         } else {
            // Summary by tapepool or vid

            for(bool is_buffer_full = false; !m_queueItor.end() && !is_buffer_full; ++m_queueItor)
            {
               uint64_t total_files = 0;
               uint64_t total_size = 0;

               for( ; !m_queueItor.end(); ++m_queueItor) {
                  auto job = m_queueItor.getJob();
                  if(job.first) {
                     ++total_files;
                     total_size += fileSize(job.second);
                  }
               }

               is_buffer_full = pushRecord(streambuf, m_queueItor.qid(), total_files, total_size);
            }
         }

         dlen = streambuf->Size();
         XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
      } catch(exception::Exception &ex) {
         throw std::runtime_error(ex.getMessage().str());
      } catch(std::exception &ex) {
         std::ostringstream errMsg;
         errMsg << __FUNCTION__ << " failed: " << ex.what();
         eInfo.Set(errMsg.str().c_str(), ECANCELED);
         delete streambuf;
      } catch(...) {
         std::ostringstream errMsg;
         errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
         eInfo.Set(errMsg.str().c_str(), ECANCELED);
         delete streambuf;
      }
      return streambuf;
   }

private:
   cta::catalogue::Catalogue &m_catalogue;                                 //!< Reference to CTA Catalogue
   bool                       m_isExtended;                                //!< Summary or extended listing?
   QueueItor_t                m_queueItor;                                 //!< Archive/Retrieve Queue Iterator

   typedef decltype(m_queueItor.getJob().second) data_t;                   //!< Infer data type from template type

   uint64_t fileSize(const data_t &job);                                   //!< Obtain file size from queue item
   bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,               //!< Convert data to protobufs and put on stream
      const std::string &tape_id, const data_t &job);
   bool pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,               //!< Convert summary to protobufs and put on stream
      const std::string &tape_id, const uint64_t &total_files,
      const uint64_t &total_size);

   static constexpr const char* const LOG_SUFFIX  = "ListPendingQueue";    //!< Identifier for log messages
};



// Template specialisations for Archive Queue types

template<>
uint64_t ListPendingQueue<OStoreDB::ArchiveQueueItor_t>::fileSize(const data_t &job) {
   return job.request.fileSize;
}

template<>
bool ListPendingQueue<OStoreDB::ArchiveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
   const std::string &tapepool, const common::dataStructures::ArchiveJob &job)
{
   Data record;

   // Response type
   record.mutable_af_item()->set_type(cta::admin::ArchiveFileItem::LISTPENDINGARCHIVES);

   // Tapepool
   record.mutable_af_item()->set_tapepool(tapepool);

   // Copy number
   record.mutable_af_item()->set_copy_nb(job.copyNumber);

   // Archive file
   auto af = record.mutable_af_item()->mutable_af();
   af->set_archive_id(job.archiveFileID);
   af->set_disk_instance(job.instanceName);
   af->set_disk_id(job.request.diskFileID);
   af->set_size(job.request.fileSize);
   af->mutable_cs()->set_type(job.request.checksumType);
   af->mutable_cs()->set_value(job.request.checksumValue);         
   af->set_storage_class(job.request.storageClass);
   af->mutable_df()->set_owner(job.request.requester.name);
   af->mutable_df()->set_group(job.request.requester.group);
   af->mutable_df()->set_path(job.request.diskFileInfo.path);

   return streambuf->Push(record);
}

template<>
bool ListPendingQueue<OStoreDB::ArchiveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
   const std::string &tapepool, const uint64_t &total_files, const uint64_t &total_size)
{
   Data record;

   // Response type
   record.mutable_af_summary_item()->set_type(cta::admin::ArchiveFileSummaryItem::LISTPENDINGARCHIVES);

   // Tapepool
   record.mutable_af_summary_item()->set_tapepool(tapepool);

   // Summary statistics
   record.mutable_af_summary_item()->set_total_files(total_files);
   record.mutable_af_summary_item()->set_total_size(total_size);

   return streambuf->Push(record);
}



// Template specialisations for Retrieve Queue types

template<>
uint64_t ListPendingQueue<OStoreDB::RetrieveQueueItor_t>::fileSize(const data_t &job) {
   return m_catalogue.getArchiveFileById(job.request.archiveFileID).fileSize;
}

template<>
bool ListPendingQueue<OStoreDB::RetrieveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
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

      // Response type
      record.mutable_af_item()->set_type(cta::admin::ArchiveFileItem::LISTPENDINGRETRIEVES);

      // Copy number
      record.mutable_af_item()->set_copy_nb(tape_it->second.first);

      // Archive file
      auto af = record.mutable_af_item()->mutable_af();
      af->set_archive_id(job.request.archiveFileID);
      //af->set_size(tape_it->second.second.compressedSize);
      af->set_size(fileSize(job));
      af->mutable_df()->set_owner(job.request.requester.name);
      af->mutable_df()->set_group(job.request.requester.group);
      af->mutable_df()->set_path(job.request.diskFileInfo.path);

      // Tape file
      auto tf = record.mutable_af_item()->mutable_tf();
      tf->set_vid(tape_it->first);
      tf->set_f_seq(tape_it->second.second.fSeq);
      tf->set_block_id(tape_it->second.second.blockId);

      is_buffer_full = streambuf->Push(record);
   }

   return is_buffer_full;
}

template<>
bool ListPendingQueue<OStoreDB::RetrieveQueueItor_t>::pushRecord(XrdSsiPb::OStreamBuffer<Data> *streambuf,
   const std::string &vid, const uint64_t &total_files, const uint64_t &total_size)
{
   Data record;

   // Response type
   record.mutable_af_summary_item()->set_type(cta::admin::ArchiveFileSummaryItem::LISTPENDINGRETRIEVES);

   // Tapepool
   record.mutable_af_summary_item()->set_vid(vid);

   // Summary statistics
   record.mutable_af_summary_item()->set_total_files(total_files);
   record.mutable_af_summary_item()->set_total_size(total_size);

   return streambuf->Push(record);
}

}} // namespace cta::xrd
