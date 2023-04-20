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

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"

namespace cta { namespace xrd {

/*!
 * Stream object which implements "recycletf ls" command
 */
class RecycleTapeFileLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  RecycleTapeFileLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return !m_fileRecycleLogItor.hasMore();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  cta::catalogue::FileRecycleLogItor m_fileRecycleLogItor;     //!< List of recycle tape files from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "RecycleTapeFileLsStream";    //!< Identifier for log messages
};


RecycleTapeFileLsStream::RecycleTapeFileLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler)
{
  using namespace cta::admin;

  bool has_any = false;
  
  cta::catalogue::RecycleTapeFileSearchCriteria searchCriteria;
  
  searchCriteria.vid = requestMsg.getOptional(OptionString::VID, &has_any);
  
  auto diskFileId = requestMsg.getOptional(OptionString::FXID, &has_any);

  searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
  
  if(diskFileId) {
    // single option on the command line we need to do the conversion ourselves.
    if(!searchCriteria.diskFileIds) searchCriteria.diskFileIds = std::vector<std::string>();
    auto fid = strtol(diskFileId->c_str(), nullptr, 16);
    if(fid < 1 || fid == LONG_MAX) {
       throw cta::exception::UserError(*diskFileId + " is not a valid file ID");
    }

    searchCriteria.diskFileIds->push_back(std::to_string(fid));
  }
  // Disk instance on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.diskInstance = requestMsg.getOptional(OptionString::INSTANCE);

  searchCriteria.archiveFileId = requestMsg.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

  // Copy number on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.copynb = requestMsg.getOptional(OptionUInt64::COPY_NUMBER);

  if(!has_any){
    throw cta::exception::UserError("Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
  }
  
  m_fileRecycleLogItor = catalogue.FileRecycleLog()->getFileRecycleLogItor(searchCriteria);
          
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "RecycleTapeFileLsStream() constructor");
}

int RecycleTapeFileLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; m_fileRecycleLogItor.hasMore() && !is_buffer_full; ) {
    const common::dataStructures::FileRecycleLog fileRecycleLog = m_fileRecycleLogItor.next();
    
    Data record;
    
    auto recycleLogToReturn = record.mutable_rtfls_item();
    recycleLogToReturn->set_vid(fileRecycleLog.vid);
    recycleLogToReturn->set_fseq(fileRecycleLog.fSeq);
    recycleLogToReturn->set_block_id(fileRecycleLog.blockId);
    recycleLogToReturn->set_copy_nb(fileRecycleLog.copyNb);
    recycleLogToReturn->set_tape_file_creation_time(fileRecycleLog.tapeFileCreationTime);
    recycleLogToReturn->set_archive_file_id(fileRecycleLog.archiveFileId);
    recycleLogToReturn->set_disk_instance(fileRecycleLog.diskInstanceName);
    recycleLogToReturn->set_disk_file_id(fileRecycleLog.diskFileId);
    recycleLogToReturn->set_disk_file_id_when_deleted(fileRecycleLog.diskFileIdWhenDeleted);
    recycleLogToReturn->set_disk_file_uid(fileRecycleLog.diskFileUid);
    recycleLogToReturn->set_disk_file_gid(fileRecycleLog.diskFileGid);
    recycleLogToReturn->set_size_in_bytes(fileRecycleLog.sizeInBytes);
    
    // Checksum
    common::ChecksumBlob csb;
    checksum::ChecksumBlobToProtobuf(fileRecycleLog.checksumBlob, csb);
    for(auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
      auto cs_ptr = recycleLogToReturn->add_checksum();
      cs_ptr->set_type(csb_it->type());
      cs_ptr->set_value(checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
    }
    recycleLogToReturn->set_storage_class(fileRecycleLog.storageClassName);
    recycleLogToReturn->set_archive_file_creation_time(fileRecycleLog.archiveFileCreationTime);
    recycleLogToReturn->set_reconciliation_time(fileRecycleLog.reconciliationTime);
    if(fileRecycleLog.collocationHint){
      recycleLogToReturn->set_collocation_hint(fileRecycleLog.collocationHint.value());
    }
    if(fileRecycleLog.diskFilePath){
      recycleLogToReturn->set_disk_file_path(fileRecycleLog.diskFilePath.value());
    }
    recycleLogToReturn->set_reason_log(fileRecycleLog.reasonLog);
    recycleLogToReturn->set_recycle_log_time(fileRecycleLog.recycleLogTime);
    // is_buffer_full is set to true when we have one full block of data in the buffer, i.e.
    // enough data to send to the client. The actual buffer size is double the block size,
    // so we can keep writing a few additional records after is_buffer_full is true. These
    // will be sent on the next iteration. If we exceed the hard limit of double the block
    // size, Push() will throw an exception.
    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
