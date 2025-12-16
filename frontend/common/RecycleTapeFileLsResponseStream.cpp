/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "RecycleTapeFileLsResponseStream.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

RecycleTapeFileLsResponseStream::RecycleTapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                 cta::Scheduler& scheduler,
                                                                 const std::string& instanceName,
                                                                 const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(adminCmd);
  bool has_any = false;

  cta::catalogue::RecycleTapeFileSearchCriteria searchCriteria;

  searchCriteria.vid = request.getOptional(OptionString::VID, &has_any);

  auto diskFileIdStr = request.getAndValidateDiskFileIdOptional(&has_any);

  searchCriteria.diskFileIds = request.getOptional(OptionStrList::FILE_ID, &has_any);
  if (diskFileIdStr) {
    if (!searchCriteria.diskFileIds) {
      searchCriteria.diskFileIds = std::vector<std::string>();
    }
    searchCriteria.diskFileIds->push_back(diskFileIdStr.value());
  }
  searchCriteria.diskInstance = request.getOptional(OptionString::INSTANCE, &has_any);
  searchCriteria.archiveFileId = request.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

  searchCriteria.recycleLogTimeMin = request.getOptional(OptionUInt64::LOG_UNIXTIME_MIN, &has_any);
  searchCriteria.recycleLogTimeMax = request.getOptional(OptionUInt64::LOG_UNIXTIME_MAX, &has_any);
  searchCriteria.vo = request.getOptional(OptionString::VO, &has_any);

  // Copy number on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.copynb = request.getOptional(OptionUInt64::COPY_NUMBER);

  if (!has_any) {
    throw cta::exception::UserError("Must specify at least one of the following search options: vid, fxid, fxidfile, "
                                    "archiveFileId, instance, vo, ltmin, ltmax");
  }

  // Get all matching file recycle logs
  m_fileRecycleLogItor = m_catalogue.FileRecycleLog()->getFileRecycleLogItor(searchCriteria);
}

bool RecycleTapeFileLsResponseStream::isDone() {
  return !m_fileRecycleLogItor.hasMore();
}

cta::xrd::Data RecycleTapeFileLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto fileRecycleLog = m_fileRecycleLogItor.next();

  cta::xrd::Data data;
  auto recycleLogToReturn = data.mutable_rtfls_item();

  recycleLogToReturn->set_instance_name(m_instanceName);
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
  cta::common::ChecksumBlob csb;
  cta::checksum::ChecksumBlobToProtobuf(fileRecycleLog.checksumBlob, csb);
  for (auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
    auto cs_ptr = recycleLogToReturn->add_checksum();
    cs_ptr->set_type(csb_it->type());
    cs_ptr->set_value(cta::checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
  }
  recycleLogToReturn->set_storage_class(fileRecycleLog.storageClassName);
  recycleLogToReturn->set_virtual_organization(fileRecycleLog.vo);
  recycleLogToReturn->set_archive_file_creation_time(fileRecycleLog.archiveFileCreationTime);
  recycleLogToReturn->set_reconciliation_time(fileRecycleLog.reconciliationTime);
  if (fileRecycleLog.collocationHint) {
    recycleLogToReturn->set_collocation_hint(fileRecycleLog.collocationHint.value());
  }
  if (fileRecycleLog.diskFilePath) {
    recycleLogToReturn->set_disk_file_path(fileRecycleLog.diskFilePath.value());
  }
  recycleLogToReturn->set_reason_log(fileRecycleLog.reasonLog);
  recycleLogToReturn->set_recycle_log_time(fileRecycleLog.recycleLogTime);

  return data;
}

}  // namespace cta::frontend