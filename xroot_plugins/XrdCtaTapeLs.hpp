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
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::xrd {

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
  const std::string m_instanceName;

  static constexpr const char * const LOG_SUFFIX = "TapeLsStream";
};


TapeLsStream::TapeLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_instanceName(requestMsg.getInstanceName())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapeLsStream() constructor");

  cta::catalogue::TapeSearchCriteria searchCriteria;
   
  bool has_any = false; // set to true if at least one optional option is set

  // Get the search criteria from the optional options

  searchCriteria.full                   = requestMsg.getOptional(OptionBoolean::FULL,                       &has_any);
  searchCriteria.fromCastor             = requestMsg.getOptional(OptionBoolean::FROM_CASTOR,                &has_any);
  searchCriteria.capacityInBytes        = requestMsg.getOptional(OptionUInt64::CAPACITY,                    &has_any);
  searchCriteria.logicalLibrary         = requestMsg.getOptional(OptionString::LOGICAL_LIBRARY,             &has_any);
  searchCriteria.tapePool               = requestMsg.getOptional(OptionString::TAPE_POOL,                   &has_any);
  searchCriteria.vo                     = requestMsg.getOptional(OptionString::VO,                          &has_any);
  searchCriteria.vid                    = requestMsg.getOptional(OptionString::VID,                         &has_any);
  searchCriteria.mediaType              = requestMsg.getOptional(OptionString::MEDIA_TYPE,                  &has_any);
  searchCriteria.vendor                 = requestMsg.getOptional(OptionString::VENDOR,                      &has_any);
  searchCriteria.purchaseOrder          = requestMsg.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER, &has_any);
  searchCriteria.physicalLibraryName    = requestMsg.getOptional(OptionString::PHYSICAL_LIBRARY,            &has_any);
  searchCriteria.diskFileIds            = requestMsg.getOptional(OptionStrList::FILE_ID,                    &has_any);
  searchCriteria.checkMissingFileCopies = requestMsg.getOptional(OptionBoolean::MISSING_FILE_COPIES,        &has_any);
  if (searchCriteria.checkMissingFileCopies.value_or(false)) {
    searchCriteria.missingFileCopiesMinAgeSecs = requestMsg.getMissingFileCopiesMinAgeSecs();
  }
  auto stateOpt                      = requestMsg.getOptional(OptionString::STATE,                       &has_any);

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
    
    fillTapeItem(tape, tape_item, m_instanceName);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
