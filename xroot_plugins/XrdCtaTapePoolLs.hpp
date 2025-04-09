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

#include "catalogue/TapePool.hpp"
#include "catalogue/TapePoolSearchCriteria.hpp"
#include "XrdCtaStream.hpp"
#include <sstream>
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class TapePoolLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  TapePoolLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_tapePoolList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::catalogue::TapePool> m_tapePoolList;                     //!< List of tape pools from the catalogue
  const std::string m_instanceName;

  static constexpr const char* const LOG_SUFFIX  = "TapePoolLsStream";    //!< Identifier for log messages
};


TapePoolLsStream::TapePoolLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_tapePoolList(catalogue.TapePool()->getTapePools()),
  m_instanceName(requestMsg.getInstanceName())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapePoolLsStream() constructor");
  cta::catalogue::TapePoolSearchCriteria searchCriteria;

  searchCriteria.name = requestMsg.getOptional(OptionString::TAPE_POOL);
  searchCriteria.vo = requestMsg.getOptional(OptionString::VO);
  searchCriteria.encrypted = requestMsg.getOptional(OptionBoolean::ENCRYPTED);
  searchCriteria.encryptionKeyName = requestMsg.getOptional(OptionString::ENCRYPTION_KEY_NAME);

  if(searchCriteria.encrypted && searchCriteria.encryptionKeyName) {
    throw exception::UserError("Do not request both '--encrypted' and '--encryptionkeyname' at same time.");
  }

  m_tapePoolList = m_catalogue.TapePool()->getTapePools(searchCriteria);
}

int TapePoolLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_tapePoolList.empty() && !is_buffer_full; m_tapePoolList.pop_front()) {
    Data record;

    auto &tp      = m_tapePoolList.front();
    auto  tp_item = record.mutable_tpls_item();

    fillTapePoolItem(tp, tp_item, m_instanceName);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
