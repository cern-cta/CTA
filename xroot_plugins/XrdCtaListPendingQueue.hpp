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
   ListPendingQueue(bool is_extended, QueueItor_t queueItor) :
      XrdSsiStream(XrdSsiStream::isActive),
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
   virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): XrdSsi buffer fill request (", dlen, " bytes)");

      XrdSsiPb::OStreamBuffer<Data> *streambuf;

      try {
         if(m_queueItor.end()) {
            // Nothing more to send, close the stream
            last = true;
            return nullptr;
         }

         streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

         for(bool is_buffer_full = false; !m_queueItor.end() && !is_buffer_full; ++m_queueItor)
         {
            auto job = m_queueItor.getJob();

            if(!job.first) continue;

            Data record = fillRecord(m_queueItor.qid(), job.second);

            // is_buffer_full is set to true when we have one full block of data in the buffer, i.e.
            // enough data to send to the client. The actual buffer size is double the block size,
            // so we can keep writing a few additional records after is_buffer_full is true. These
            // will be sent on the next iteration. If we exceed the hard limit of double the block
            // size, Push() will throw an exception.
            is_buffer_full = streambuf->Push(record);
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
   bool        m_isExtended;
   QueueItor_t m_queueItor;

   typedef decltype(m_queueItor.getJob().second) data_t;                   //!< Infer data type from template type

   Data fillRecord(const std::string &tape_id, const data_t &job);         //!< Convert data to protobuf

   static constexpr const char* const LOG_SUFFIX  = "ListPendingQueue";    //!< Identifier for log messages
};



// Template specialisations for Archive and Retrieve Queue types

template<>
Data ListPendingQueue<OStoreDB::ArchiveQueueItor_t>::fillRecord(const std::string &tapepool, const common::dataStructures::ArchiveJob &job)
{
   Data record;

   // Tapepool
   record.mutable_af_ls_item()->set_tapepool(tapepool);

   // Copy number
   record.mutable_af_ls_item()->set_copy_nb(job.copyNumber);

   // Archive file
   auto af = record.mutable_af_ls_item()->mutable_af();
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

#if 0
   if(m_isExtended)
   {

   } else {
      auto lpa = record.mutable_lpa_summary();
      lpa->set_
   }
#endif
#if 0
               // Copy number
               record.mutable_af_ls_item()->set_copy_nb(jt->first);

               // Archive file
               auto af = record.mutable_af_ls_item()->mutable_af();
               af->set_archive_id(archiveFile.archiveFileID);
               af->set_disk_instance(archiveFile.diskInstance);
               af->set_disk_id(archiveFile.diskFileId);
               af->set_size(archiveFile.fileSize);
               af->mutable_cs()->set_type(archiveFile.checksumType);
               af->mutable_cs()->set_value(archiveFile.checksumValue);
               af->set_storage_class(archiveFile.storageClass);
               af->mutable_df()->set_owner(archiveFile.diskFileInfo.owner);
               af->mutable_df()->set_group(archiveFile.diskFileInfo.group);
               af->mutable_df()->set_path(archiveFile.diskFileInfo.path);
               af->set_creation_time(archiveFile.creationTime);

               // Tape file
               auto tf = record.mutable_af_ls_item()->mutable_tf();
               tf->set_vid(jt->second.vid);
               tf->set_f_seq(jt->second.fSeq);
               tf->set_block_id(jt->second.blockId);
#endif
#if 0
   std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob>> result;

      if(has_flag(OptionBoolean::EXTENDED))
      {
         for(auto it = result.cbegin(); it != result.cend(); it++) {
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               std::vector<std::string> currentRow;
               currentRow.push_back(it->first);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->archiveFileID)));
               currentRow.push_back(jt->request.storageClass);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->copyNumber)));
               currentRow.push_back(jt->request.diskFileID);
               currentRow.push_back(jt->instanceName);
               currentRow.push_back(jt->request.checksumType);
               currentRow.push_back(jt->request.checksumValue);         
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->request.fileSize)));
               currentRow.push_back(jt->request.requester.name);
               currentRow.push_back(jt->request.requester.group);
               currentRow.push_back(jt->request.diskFileInfo.path);
               responseTable.push_back(currentRow);
            }
         }
      } else {
         for(auto it = result.cbegin(); it != result.cend(); it++) {
            std::vector<std::string> currentRow;
            currentRow.push_back(it->first);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->second.size())));
            uint64_t size = 0;
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
               size += jt->request.fileSize;
            }
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(size)));
            responseTable.push_back(currentRow);
         }
      }
#endif

   return record;
}

}} // namespace cta::xrd
