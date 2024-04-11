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

#include "catalogue/CatalogueItor.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "frontend/common/GrpcEndpoint.hpp"
#include "XrdCtaStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapefile ls" command.
 */
class TapeFileLsStream : public XrdCtaStream {
public:
  TapeFileLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  bool isDone() const override {
    return !m_tapeFileItor.hasMore();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  catalogue::ArchiveFileItor m_tapeFileItor;       //!< Iterator across files which have been archived

  static constexpr const char* const LOG_SUFFIX  = "TapeFileLsStream";    //!< Identifier for log messages
};


TapeFileLsStream::TapeFileLsStream(const frontend::AdminCmdStream& requestMsg,
  cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
    XrdCtaStream(catalogue, scheduler)
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapeFileLsStream() constructor");

  bool has_any = false; // set to true if at least one optional option is set

  // Get the search criteria from the optional options
  cta::catalogue::TapeFileSearchCriteria searchCriteria;

  searchCriteria.vid = requestMsg.getOptional(OptionString::VID, &has_any);
  // Disk file IDs can be a list or a single ID
  auto diskFileId = requestMsg.getOptional(OptionString::FXID, &has_any);
  searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
  if(diskFileId.has_value()) {
    if (auto fid = diskFileId.value(); !utils::isValidID(fid)) {
      throw cta::exception::UserError(fid + " is not a valid file ID");
    }
    if(!searchCriteria.diskFileIds) searchCriteria.diskFileIds = std::vector<std::string>();
    searchCriteria.diskFileIds->push_back(diskFileId.value());
  }
  searchCriteria.diskInstance = requestMsg.getOptional(OptionString::INSTANCE, &has_any);

  searchCriteria.archiveFileId = requestMsg.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

  if(!has_any) {
    throw cta::exception::UserError("Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
  }

  m_tapeFileItor = m_catalogue.ArchiveFile()->getArchiveFilesItor(searchCriteria);
}


int TapeFileLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; m_tapeFileItor.hasMore() && !is_buffer_full; )
  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_tapeFileItor.next();

    for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
      Data record;

      // Archive file
      auto af = record.mutable_tfls_item()->mutable_af();
      af->set_archive_id(archiveFile.archiveFileID);
      af->set_storage_class(archiveFile.storageClass);
      af->set_creation_time(archiveFile.creationTime);
      af->set_size(archiveFile.fileSize);

      // Checksum
      common::ChecksumBlob csb;
      checksum::ChecksumBlobToProtobuf(archiveFile.checksumBlob, csb);
      for(auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
        auto cs_ptr = af->add_checksum();
        cs_ptr->set_type(csb_it->type());
        cs_ptr->set_value(checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
      }

      // Disk file
      auto df = record.mutable_tfls_item()->mutable_df();
      df->set_disk_id(archiveFile.diskFileId);
      df->set_disk_instance(archiveFile.diskInstance);
      df->mutable_owner_id()->set_uid(archiveFile.diskFileInfo.owner_uid);
      df->mutable_owner_id()->set_gid(archiveFile.diskFileInfo.gid);

      // Tape file
      auto tf = record.mutable_tfls_item()->mutable_tf();
      tf->set_vid(jt->vid);
      tf->set_copy_nb(jt->copyNb);
      tf->set_block_id(jt->blockId);
      tf->set_f_seq(jt->fSeq);

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

} // namespace cta::xrd
