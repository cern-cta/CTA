/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Tape File Ls stream implementation
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
#include <xroot_plugins/GrpcEndpoint.hpp>
#include <common/checksum/ChecksumBlobSerDeser.hpp>


namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapefile ls" command.
 */
class TapeFileLsStream : public XrdCtaStream
{
public:
  // Constructor when we need gRPC namespace lookups
  TapeFileLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler,
                   const NamespaceMap_t &nsMap);

  // Constructor when we do not require namespace lookups
  TapeFileLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
    TapeFileLsStream(requestMsg, catalogue, scheduler, NamespaceMap_t()) {
    m_LookupNamespace = false;
  }

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return !m_tapeFileItor.hasMore();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  catalogue::ArchiveFileItor    m_tapeFileItor;       //!< Iterator across files which have been archived
  grpc::EndpointMap             m_endpoints;          //!< List of gRPC endpoints
  bool                          m_LookupNamespace;    //!< True if namespace lookup is required
  bool                          m_ShowSuperseded;     //!< True if superseded files should be included in the output

  static constexpr const char* const LOG_SUFFIX  = "TapeFileLsStream";    //!< Identifier for log messages
};


TapeFileLsStream::TapeFileLsStream(const RequestMessage &requestMsg,
  cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler,
  const NamespaceMap_t &nsMap) :
    XrdCtaStream(catalogue, scheduler),
    m_endpoints(nsMap)
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapeFileLsStream() constructor");

  m_LookupNamespace = true;
  m_ShowSuperseded  = requestMsg.has_flag(OptionBoolean::SHOW_SUPERSEDED);


  bool has_any = false; // set to true if at least one optional option is set

  // Get the search criteria from the optional options
  cta::catalogue::TapeFileSearchCriteria searchCriteria;

  searchCriteria.vid         = requestMsg.getOptional(OptionString::VID,      &has_any);

  // Disk file IDs can be a list or a single ID
  searchCriteria.diskFileIds = requestMsg.getOptional(OptionStrList::FILE_ID, &has_any);
  auto diskFileId            = requestMsg.getOptional(OptionStr::FID,         &has_any);
  if(diskFileId) searchCriteria.diskFileIds.push_back(*diskFileId);

  if(!has_any) {
    throw cta::exception::UserError("Must specify at least one search option");
  }

  m_tapeFileItor = m_catalogue.getArchiveFilesItor(searchCriteria);
}


int TapeFileLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; m_tapeFileItor.hasMore() && !is_buffer_full; )
  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_tapeFileItor.next();

    for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
      Data record;

      // Skip superseded files unless explicitly requested to show them
      if(!(m_ShowSuperseded || jt->supersededByVid.empty())) continue;

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
      if(m_LookupNamespace) {
        df->set_path(m_endpoints.getPath(archiveFile.diskInstance, archiveFile.diskFileId));
      }

      // Tape file
      auto tf = record.mutable_tfls_item()->mutable_tf();
      tf->set_vid(jt->vid);
      tf->set_copy_nb(jt->copyNb);
      tf->set_block_id(jt->blockId);
      tf->set_f_seq(jt->fSeq);
      tf->set_superseded_by_f_seq(jt->supersededByFSeq);
      tf->set_superseded_by_vid(jt->supersededByVid);

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
