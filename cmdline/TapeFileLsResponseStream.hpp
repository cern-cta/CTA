#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/exception/UserError.hpp"

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class TapeFileLsResponseStream : public CtaAdminResponseStream {
public:
  TapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const frontend::AdminCmdStream& requestMsg);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  catalogue::ArchiveFileItor m_tapeFileItor;
  std::optional<common::dataStructures::ArchiveFile> m_currentArchiveFile;
  std::list<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileIter;
  std::list<common::dataStructures::TapeFile>::const_iterator m_currentTapeFileEnd;

  void validateSearchCriteria(bool hasAnySearchOption) const;
  void populateDataItem(cta::xrd::Data& data,
                        const common::dataStructures::ArchiveFile& archiveFile,
                        const common::dataStructures::TapeFile& tapeFile) const;
};

TapeFileLsResponseStream::TapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                   cta::Scheduler& scheduler,
                                                   const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()) {
  const admin::AdminCmd& admincmd = requestMsg.getAdminCmd();
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(admincmd);
  bool has_any = false;

  cta::catalogue::TapeFileSearchCriteria searchCriteria;

  searchCriteria.vid = request.getOptional(OptionString::VID, &has_any);

  // Handle disk file IDs (can be list or single ID)
  std::optional<std::string> diskFileIdStr;
  try {
    diskFileIdStr = request.getAndValidateDiskFileIdOptional(&has_any);
  } catch (const cta::exception::UserError& ex) {
    throw cta::exception::UserError(ex.getMessageValue());
  }
  searchCriteria.diskFileIds = request.getOptional(OptionStrList::FILE_ID, &has_any);
  if (diskFileIdStr) {
    if (!searchCriteria.diskFileIds) {
      searchCriteria.diskFileIds = std::vector<std::string>();
    }
    searchCriteria.diskFileIds->push_back(diskFileIdStr.value());
  }

  searchCriteria.diskInstance = request.getOptional(OptionString::INSTANCE, &has_any);
  searchCriteria.archiveFileId = request.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);

  validateSearchCriteria(has_any);

  m_tapeFileItor = m_catalogue.ArchiveFile()->getArchiveFilesItor(searchCriteria);
  m_currentArchiveFile = std::nullopt;
}

bool TapeFileLsResponseStream::isDone() {
  // If we have a current archive file with tape files to process, we're not done
  if (m_currentArchiveFile && m_currentTapeFileIter != m_currentTapeFileEnd) {
    return false;
  }

  // Try to get the next archive file if we don't have one or finished processing current one
  if (!m_currentArchiveFile || m_currentTapeFileIter == m_currentTapeFileEnd) {
    if (m_tapeFileItor.hasMore()) {
      m_currentArchiveFile = m_tapeFileItor.next();
      m_currentTapeFileIter = m_currentArchiveFile->tapeFiles.cbegin();
      m_currentTapeFileEnd = m_currentArchiveFile->tapeFiles.cend();
      return m_currentTapeFileIter == m_currentTapeFileEnd;
    } else {
      return true;
    }
  }

  return false;
}

cta::xrd::Data TapeFileLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  cta::xrd::Data data;
  populateDataItem(data, *m_currentArchiveFile, *m_currentTapeFileIter);

  ++m_currentTapeFileIter;

  return data;
}

void TapeFileLsResponseStream::populateDataItem(cta::xrd::Data& data,
                                                const common::dataStructures::ArchiveFile& archiveFile,
                                                const common::dataStructures::TapeFile& tapeFile) const {
  cta::admin::TapeFileLsItem* tf_item = data.mutable_tfls_item();

  // Set the instance name
  tf_item->set_instance_name(m_instanceName);

  // Archive file info
  auto af = tf_item->mutable_af();
  af->set_archive_id(archiveFile.archiveFileID);
  af->set_storage_class(archiveFile.storageClass);
  af->set_creation_time(archiveFile.creationTime);
  af->set_size(archiveFile.fileSize);

  // Checksum
  common::ChecksumBlob csb;
  checksum::ChecksumBlobToProtobuf(archiveFile.checksumBlob, csb);
  for (auto csb_it = csb.cs().begin(); csb_it != csb.cs().end(); ++csb_it) {
    auto cs_ptr = af->add_checksum();
    cs_ptr->set_type(csb_it->type());
    cs_ptr->set_value(checksum::ChecksumBlob::ByteArrayToHex(csb_it->value()));
  }

  // Disk file info
  auto df = tf_item->mutable_df();
  df->set_disk_id(archiveFile.diskFileId);
  df->set_disk_instance(archiveFile.diskInstance);
  df->mutable_owner_id()->set_uid(archiveFile.diskFileInfo.owner_uid);
  df->mutable_owner_id()->set_gid(archiveFile.diskFileInfo.gid);

  // Tape file info
  auto tf = tf_item->mutable_tf();
  tf->set_vid(tapeFile.vid);
  tf->set_copy_nb(tapeFile.copyNb);
  tf->set_block_id(tapeFile.blockId);
  tf->set_f_seq(tapeFile.fSeq);
}

void TapeFileLsResponseStream::validateSearchCriteria(bool hasAnySearchOption) const {
  if (!hasAnySearchOption) {
    throw cta::exception::UserError(
      "Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
  }
}

}  // namespace cta::cmdline