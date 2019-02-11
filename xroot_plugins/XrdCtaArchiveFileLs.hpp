/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Archive File Ls stream implementation
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
#include <catalogue/Catalogue.hpp>



namespace cta { namespace xrd {

/*!
 * Stream object which implements "af ls" command.
 */
class ArchiveFileLsStream : public XrdSsiStream
{
public:
  ArchiveFileLsStream(cta::catalogue::Catalogue &catalogue, const cta::catalogue::TapeFileSearchCriteria &searchCriteria, bool is_summary) :
    XrdSsiStream(XrdSsiStream::isActive),
    m_catalogue(catalogue),
    m_searchCriteria(searchCriteria),
    m_isSummary(is_summary)
  {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ArchiveFileLsStream() constructor");

    if(!m_isSummary) {
      m_archiveFileItor = m_catalogue.getArchiveFilesItor(searchCriteria);
    }
  }

  virtual ~ArchiveFileLsStream() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~ArchiveFileLsStream() destructor");
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
      if(!m_isSummary && !m_archiveFileItor.hasMore()) {
        // Nothing more to send, close the stream
        last = true;
        return nullptr;
      }

      streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

      // Special handling for -S option
      if(m_isSummary) {
        GetBuffSummary(streambuf);
        dlen = streambuf->Size();
        last = true;
        return streambuf;
      }

      for(bool is_buffer_full = false; m_archiveFileItor.hasMore() && !is_buffer_full; )
      {
        const cta::common::dataStructures::ArchiveFile archiveFile = m_archiveFileItor.next();

        for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
          Data record;

          // Copy number
          record.mutable_afls_item()->set_copy_nb(jt->first);

          // Archive file
          auto af = record.mutable_afls_item()->mutable_af();
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
          auto tf = record.mutable_afls_item()->mutable_tf();
          tf->set_vid(jt->second.vid);
          tf->set_f_seq(jt->second.fSeq);
          tf->set_block_id(jt->second.blockId);

          // is_buffer_full is set to true when we have one full block of data in the buffer, i.e.
          // enough data to send to the client. The actual buffer size is double the block size,
          // so we can keep writing a few additional records after is_buffer_full is true. These
          // will be sent on the next iteration. If we exceed the hard limit of double the block
          // size, Push() will throw an exception.
          is_buffer_full = streambuf->Push(record);
        }
      }
      dlen = streambuf->Size();
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
    } catch(cta::exception::Exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught CTA exception: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      delete streambuf;
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

  void GetBuffSummary(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
    common::dataStructures::ArchiveFileSummary summary = m_catalogue.getTapeFileSummary(m_searchCriteria);

    Data record;

    // Summary statistics
    record.mutable_afls_summary()->set_total_files(summary.totalFiles);
    record.mutable_afls_summary()->set_total_size(summary.totalBytes);

    streambuf->Push(record);

    m_isSummary = false;
  }

private:
  cta::catalogue::Catalogue                    &m_catalogue;                 //!< Reference to CTA Catalogue
  const cta::catalogue::TapeFileSearchCriteria  m_searchCriteria;            //!< Search criteria
  catalogue::ArchiveFileItor                    m_archiveFileItor;           //!< Iterator across files which have been archived
  bool                                          m_isSummary;                 //!< Full listing or short summary?

  static constexpr const char* const LOG_SUFFIX  = "ArchiveFileLsStream";    //!< Identifier for log messages
};

}} // namespace cta::xrd
