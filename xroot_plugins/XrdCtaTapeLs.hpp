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

#include "XrdCtaStream.hpp"

namespace cta { namespace xrd {

/*!
 * Stream object which implements "tape ls" command
 */
class TapeLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  TapeLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_tapeList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);
  
  std::list<common::dataStructures::Tape> m_tapeList;

  static constexpr const char * const LOG_SUFFIX = "TapeLsStream";
};


TapeLsStream::TapeLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler)
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapeLsStream() constructor");

  cta::catalogue::TapeSearchCriteria searchCriteria;
   
  bool has_any = false; // set to true if at least one optional option is set

  // Get the search criteria from the optional options

  searchCriteria.full            = requestMsg.getOptional(OptionBoolean::FULL,                       &has_any);
  searchCriteria.fromCastor      = requestMsg.getOptional(OptionBoolean::FROM_CASTOR,                &has_any);
  searchCriteria.capacityInBytes = requestMsg.getOptional(OptionUInt64::CAPACITY,                    &has_any);
  searchCriteria.logicalLibrary  = requestMsg.getOptional(OptionString::LOGICAL_LIBRARY,             &has_any);
  searchCriteria.tapePool        = requestMsg.getOptional(OptionString::TAPE_POOL,                   &has_any);
  searchCriteria.vo              = requestMsg.getOptional(OptionString::VO,                          &has_any);
  searchCriteria.vid             = requestMsg.getOptional(OptionString::VID,                         &has_any);
  searchCriteria.mediaType       = requestMsg.getOptional(OptionString::MEDIA_TYPE,                  &has_any);
  searchCriteria.vendor          = requestMsg.getOptional(OptionString::VENDOR,                      &has_any);
  searchCriteria.purchaseOrder   = requestMsg.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
  searchCriteria.diskFileIds     = requestMsg.getOptional(OptionStrList::FILE_ID,                    &has_any);
  auto stateOpt                  = requestMsg.getOptional(OptionString::STATE,                       &has_any);
  if(stateOpt){
    searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value(), true);
  }
  if(!(requestMsg.has_flag(OptionBoolean::ALL) || has_any)) {
    throw cta::exception::UserError("Must specify at least one search option, or --all");
  } else if(requestMsg.has_flag(OptionBoolean::ALL) && has_any) {
    throw cta::exception::UserError("Cannot specify --all together with other search options");
  }

  m_tapeList = m_catalogue.Tape()->getTapes(searchCriteria);
}


int TapeLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_tapeList.empty() && !is_buffer_full; m_tapeList.pop_front()) {
    Data record;
    auto &tape = m_tapeList.front();
    auto tape_item = record.mutable_tals_item();
    
    tape_item->set_vid(tape.vid);
    tape_item->set_media_type(tape.mediaType);
    tape_item->set_vendor(tape.vendor);
    tape_item->set_logical_library(tape.logicalLibraryName);
    tape_item->set_tapepool(tape.tapePoolName);
    tape_item->set_vo(tape.vo);
    tape_item->set_encryption_key_name((bool)tape.encryptionKeyName ? tape.encryptionKeyName.value() : "-");
    tape_item->set_capacity(tape.capacityInBytes);
    tape_item->set_occupancy(tape.dataOnTapeInBytes);
    tape_item->set_last_fseq(tape.lastFSeq);
    tape_item->set_full(tape.full);
    tape_item->set_dirty(tape.dirty);
    tape_item->set_from_castor(tape.isFromCastor);
    tape_item->set_read_mount_count(tape.readMountCount);
    tape_item->set_write_mount_count(tape.writeMountCount);
    tape_item->set_nb_master_files(tape.nbMasterFiles);
    tape_item->set_master_data_in_bytes(tape.masterDataInBytes);
    
    if(tape.labelLog) {
      ::cta::common::TapeLog * labelLog = tape_item->mutable_label_log();
      labelLog->set_drive(tape.labelLog.value().drive);
      labelLog->set_time(tape.labelLog.value().time);
    }
    if(tape.lastWriteLog){
      ::cta::common::TapeLog * lastWriteLog = tape_item->mutable_last_written_log();
      lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
      lastWriteLog->set_time(tape.lastWriteLog.value().time);
    }
    if(tape.lastReadLog){
      ::cta::common::TapeLog * lastReadLog = tape_item->mutable_last_read_log();
      lastReadLog->set_drive(tape.lastReadLog.value().drive);
      lastReadLog->set_time(tape.lastReadLog.value().time);
    }
    ::cta::common::EntryLog * creationLog = tape_item->mutable_creation_log();
    creationLog->set_username(tape.creationLog.username);
    creationLog->set_host(tape.creationLog.host);
    creationLog->set_time(tape.creationLog.time);
    ::cta::common::EntryLog * lastModificationLog = tape_item->mutable_last_modification_log();
    lastModificationLog->set_username(tape.lastModificationLog.username);
    lastModificationLog->set_host(tape.lastModificationLog.host);
    lastModificationLog->set_time(tape.lastModificationLog.time);
    tape_item->set_comment(tape.comment);

    tape_item->set_state(tape.getStateStr());
    tape_item->set_state_reason(tape.stateReason ? tape.stateReason.value() : "");
    tape_item->set_purchase_order(tape.purchaseOrder ? tape.purchaseOrder.value() : "");
    tape_item->set_state_update_time(tape.stateUpdateTime);
    tape_item->set_state_modified_by(tape.stateModifiedBy);
    if (tape.verificationStatus) {
      tape_item->set_verification_status(tape.verificationStatus.value());
    }

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}}
