/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Archive File Ls stream implementation
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
 * Stream object which implements "archivefile ls" command.
 */
class ArchiveFileLsStream : public XrdCtaStream
{
public:
  ArchiveFileLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_isSummary ? m_isSummaryDone : !m_archiveFileItor.hasMore();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  cta::catalogue::TapeFileSearchCriteria  m_searchCriteria;                  //!< Search criteria
  catalogue::ArchiveFileItor              m_archiveFileItor;                 //!< Iterator across files which have been archived
  bool                                    m_isSummary;                       //!< Full listing or short summary?
  bool                                    m_isSummaryDone;                   //!< Summary has been sent

  static constexpr const char* const LOG_SUFFIX  = "ArchiveFileLsStream";    //!< Identifier for log messages
};


ArchiveFileLsStream::ArchiveFileLsStream(const RequestMessage &requestMsg,
  cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
    XrdCtaStream(catalogue, scheduler),
    m_isSummary(requestMsg.has_flag(admin::OptionBoolean::SUMMARY)),
    m_isSummaryDone(false)
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ArchiveFileLsStream() constructor");

  if(!requestMsg.has_flag(OptionBoolean::ALL))
  {
    bool has_any = false; // set to true if at least one optional option is set

    // Get the search criteria from the optional options

    m_searchCriteria.archiveFileId  = requestMsg.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);
    m_searchCriteria.tapeFileCopyNb = requestMsg.getOptional(OptionUInt64::COPY_NUMBER,     &has_any);
    m_searchCriteria.diskFileId     = requestMsg.getOptional(OptionString::DISKID,          &has_any);
    m_searchCriteria.vid            = requestMsg.getOptional(OptionString::VID,             &has_any);
    m_searchCriteria.tapePool       = requestMsg.getOptional(OptionString::TAPE_POOL,       &has_any);
    m_searchCriteria.diskFileUser   = requestMsg.getOptional(OptionString::OWNER,           &has_any);
    m_searchCriteria.diskFileGroup  = requestMsg.getOptional(OptionString::GROUP,           &has_any);
    m_searchCriteria.storageClass   = requestMsg.getOptional(OptionString::STORAGE_CLASS,   &has_any);
    m_searchCriteria.diskFilePath   = requestMsg.getOptional(OptionString::PATH,            &has_any);
    m_searchCriteria.diskInstance   = requestMsg.getOptional(OptionString::INSTANCE,        &has_any);

    if(!has_any) {
      throw cta::exception::UserError("Must specify at least one search option, or --all");
    }
  }

  if(!m_isSummary) {
    m_archiveFileItor = m_catalogue.getArchiveFilesItor(m_searchCriteria);
  }
}


int ArchiveFileLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  // Special handling for -S (Summary) option
  if(m_isSummary) {
    common::dataStructures::ArchiveFileSummary summary = m_catalogue.getTapeFileSummary(m_searchCriteria);

    Data record;
    record.mutable_afls_summary()->set_total_files(summary.totalFiles);
    record.mutable_afls_summary()->set_total_size(summary.totalBytes);
    streambuf->Push(record);

    m_isSummaryDone = true;
    return streambuf->Size();
  }

  for(bool is_buffer_full = false; m_archiveFileItor.hasMore() && !is_buffer_full; )
  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_archiveFileItor.next();

    for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
      Data record;

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
      tf->set_vid(jt->vid);
      tf->set_f_seq(jt->fSeq);
      tf->set_block_id(jt->blockId);
      tf->set_superseded_by_vid(jt->supersededByVid);
      tf->set_superseded_by_f_seq(jt->supersededByFSeq);
      record.mutable_afls_item()->set_copy_nb(jt->copyNb);

      // is_buffer_full is set to true when we have one full block of data in the buffer, i.e.
      // enough data to send to the client. The actual buffer size is double the block size,
      // so we can keep writing a few additional records after is_buffer_full is true. These
      // will be sent on the next iteration. If we exceed the hard limit of double the block
      // size, Push() will throw an exception.
      is_buffer_full = streambuf->Push(record);
    }
  }

  return streambuf->Size();
}

}} // namespace cta::xrd
