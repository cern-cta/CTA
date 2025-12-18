/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeLsResponseStream.hpp"

#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

TapeLsResponseStream::TapeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                           cta::Scheduler& scheduler,
                                           const std::string& instanceName,
                                           const admin::AdminCmd& adminCmd,
                                           uint64_t missingFileCopiesMinAgeSecs)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_missingFileCopiesMinAgeSecs(missingFileCopiesMinAgeSecs) {
  const admin::AdminCmd& admincmd = adminCmd;
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(admincmd);
  bool has_any = false;  // set to true if at least one optional option is set

  // Get the search criteria from the optional options
  m_searchCriteria.full = request.getOptional(OptionBoolean::FULL, &has_any);
  m_searchCriteria.fromCastor = request.getOptional(OptionBoolean::FROM_CASTOR, &has_any);
  m_searchCriteria.capacityInBytes = request.getOptional(OptionUInt64::CAPACITY, &has_any);
  m_searchCriteria.logicalLibrary = request.getOptional(OptionString::LOGICAL_LIBRARY, &has_any);
  m_searchCriteria.tapePool = request.getOptional(OptionString::TAPE_POOL, &has_any);
  m_searchCriteria.vo = request.getOptional(OptionString::VO, &has_any);
  m_searchCriteria.vid = request.getOptional(OptionString::VID, &has_any);
  m_searchCriteria.mediaType = request.getOptional(OptionString::MEDIA_TYPE, &has_any);
  m_searchCriteria.vendor = request.getOptional(OptionString::VENDOR, &has_any);
  m_searchCriteria.purchaseOrder = request.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
  m_searchCriteria.physicalLibraryName = request.getOptional(OptionString::PHYSICAL_LIBRARY, &has_any);
  m_searchCriteria.diskFileIds = request.getOptional(OptionStrList::FILE_ID, &has_any);

  // Handle missing file copies (this is not handled by the requestMessage as it is the Frontend that provides this option)
  m_searchCriteria.checkMissingFileCopies = request.getOptional(OptionBoolean::MISSING_FILE_COPIES, &has_any);
  if (m_searchCriteria.checkMissingFileCopies.value_or(false)) {
    m_searchCriteria.missingFileCopiesMinAgeSecs = m_missingFileCopiesMinAgeSecs;
  }

  // Handle state option
  if (auto stateOpt = request.getOptional(OptionString::STATE, &has_any); stateOpt) {
    m_searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
  }

  // Validate the search criteria
  bool hasAllFlag = request.has_flag(OptionBoolean::ALL);
  validateSearchCriteria(hasAllFlag, has_any);

  // Execute the search and get the tapes
  m_tapes = m_catalogue.Tape()->getTapes(m_searchCriteria);
}

bool TapeLsResponseStream::isDone() {
  return m_tapes.empty();
}

cta::xrd::Data TapeLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto tape = m_tapes.front();
  m_tapes.pop_front();

  cta::xrd::Data data;
  auto tapeItem = data.mutable_tals_item();

  tapeItem->set_instance_name(m_instanceName);
  tapeItem->set_vid(tape.vid);
  tapeItem->set_media_type(tape.mediaType);
  tapeItem->set_vendor(tape.vendor);
  tapeItem->set_logical_library(tape.logicalLibraryName);
  tapeItem->set_tapepool(tape.tapePoolName);
  tapeItem->set_vo(tape.vo);
  tapeItem->set_encryption_key_name(tape.encryptionKeyName.value_or(""));
  tapeItem->set_capacity(tape.capacityInBytes);
  tapeItem->set_occupancy(tape.dataOnTapeInBytes);
  tapeItem->set_last_fseq(tape.lastFSeq);
  tapeItem->set_full(tape.full);
  tapeItem->set_dirty(tape.dirty);
  tapeItem->set_from_castor(tape.isFromCastor);
  tapeItem->set_label_format(cta::admin::LabelFormatToProtobuf(tape.labelFormat));
  tapeItem->set_read_mount_count(tape.readMountCount);
  tapeItem->set_write_mount_count(tape.writeMountCount);
  tapeItem->set_nb_master_files(tape.nbMasterFiles);
  tapeItem->set_master_data_in_bytes(tape.masterDataInBytes);

  if (tape.labelLog) {
    ::cta::common::TapeLog* labelLog = tapeItem->mutable_label_log();
    labelLog->set_drive(tape.labelLog.value().drive);
    labelLog->set_time(tape.labelLog.value().time);
  }
  if (tape.lastWriteLog) {
    ::cta::common::TapeLog* lastWriteLog = tapeItem->mutable_last_written_log();
    lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
    lastWriteLog->set_time(tape.lastWriteLog.value().time);
  }
  if (tape.lastReadLog) {
    ::cta::common::TapeLog* lastReadLog = tapeItem->mutable_last_read_log();
    lastReadLog->set_drive(tape.lastReadLog.value().drive);
    lastReadLog->set_time(tape.lastReadLog.value().time);
  }
  ::cta::common::EntryLog* creationLog = tapeItem->mutable_creation_log();
  creationLog->set_username(tape.creationLog.username);
  creationLog->set_host(tape.creationLog.host);
  creationLog->set_time(tape.creationLog.time);
  ::cta::common::EntryLog* lastModificationLog = tapeItem->mutable_last_modification_log();
  lastModificationLog->set_username(tape.lastModificationLog.username);
  lastModificationLog->set_host(tape.lastModificationLog.host);
  lastModificationLog->set_time(tape.lastModificationLog.time);
  tapeItem->set_comment(tape.comment);

  tapeItem->set_state(tape.getStateStr());
  tapeItem->set_state_reason(tape.stateReason.value_or(""));
  tapeItem->set_purchase_order(tape.purchaseOrder.value_or(""));
  tapeItem->set_physical_library(tape.physicalLibraryName.value_or(""));
  tapeItem->set_state_update_time(tape.stateUpdateTime);
  tapeItem->set_state_modified_by(tape.stateModifiedBy);
  if (tape.verificationStatus) {
    tapeItem->set_verification_status(tape.verificationStatus.value());
  }

  return data;
}

void TapeLsResponseStream::validateSearchCriteria(bool hasAllFlag, bool hasAnySearchOption) const {
  if (!(hasAllFlag || hasAnySearchOption)) {
    throw cta::exception::UserError("Must specify at least one search option, or --all");
  } else if (hasAllFlag && hasAnySearchOption) {
    throw cta::exception::UserError("Cannot specify --all together with other search options");
  }
}

}  // namespace cta::frontend
