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
#include "XrdSsiCtaRequestMessage.hpp"

namespace cta { namespace xrd {

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
  TapePoolLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

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

  static constexpr const char* const LOG_SUFFIX  = "TapePoolLsStream";    //!< Identifier for log messages
};


TapePoolLsStream::TapePoolLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_tapePoolList(catalogue.TapePool()->getTapePools())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapePoolLsStream() constructor");
  cta::catalogue::TapePoolSearchCriteria searchCriteria;

  searchCriteria.name = requestMsg.getOptional(OptionString::TAPE_POOL);
  searchCriteria.vo = requestMsg.getOptional(OptionString::VO);
  searchCriteria.encrypted = requestMsg.getOptional(OptionBoolean::ENCRYPTED);

  m_tapePoolList = m_catalogue.TapePool()->getTapePools(searchCriteria);
}

int TapePoolLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_tapePoolList.empty() && !is_buffer_full; m_tapePoolList.pop_front()) {
    Data record;

    auto &tp      = m_tapePoolList.front();
    auto  tp_item = record.mutable_tpls_item();

    tp_item->set_name(tp.name);
    tp_item->set_vo(tp.vo.name);
    tp_item->set_num_tapes(tp.nbTapes);
    tp_item->set_num_partial_tapes(tp.nbPartialTapes);
    tp_item->set_num_physical_files(tp.nbPhysicalFiles);
    tp_item->set_capacity_bytes(tp.capacityBytes);
    tp_item->set_data_bytes(tp.dataBytes);
    tp_item->set_encrypt(tp.encryption);
    tp_item->set_supply(tp.supply ? tp.supply.value() : "");
    tp_item->mutable_created()->set_username(tp.creationLog.username);
    tp_item->mutable_created()->set_host(tp.creationLog.host);
    tp_item->mutable_created()->set_time(tp.creationLog.time);
    tp_item->mutable_modified()->set_username(tp.lastModificationLog.username);
    tp_item->mutable_modified()->set_host(tp.lastModificationLog.host);
    tp_item->mutable_modified()->set_time(tp.lastModificationLog.time);
    tp_item->set_comment(tp.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
