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

#include "common/dataStructures/RequesterMountRule.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"
#include "xroot_plugins/XrdSsiCtaRequestMessage.hpp"

namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class RequesterMountRuleLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  RequesterMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_requesterMountRuleList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::RequesterMountRule> m_requesterMountRuleList;    //!< List of requester mount rules from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "RequesterMountRuleLsStream";          //!< Identifier for log messages
};


RequesterMountRuleLsStream::RequesterMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_requesterMountRuleList(catalogue.RequesterMountRule()->getRequesterMountRules())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "RequesterMountRuleLsStream() constructor");
}

int RequesterMountRuleLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_requesterMountRuleList.empty() && !is_buffer_full; m_requesterMountRuleList.pop_front()) {
    Data record;

    auto &rmr      = m_requesterMountRuleList.front();
    auto  rmr_item = record.mutable_rmrls_item();

    rmr_item->set_disk_instance(rmr.diskInstance);
    rmr_item->set_requester_mount_rule(rmr.name);
    rmr_item->set_mount_policy(rmr.mountPolicy);
    rmr_item->mutable_creation_log()->set_username(rmr.creationLog.username);
    rmr_item->mutable_creation_log()->set_host(rmr.creationLog.host);
    rmr_item->mutable_creation_log()->set_time(rmr.creationLog.time);
    rmr_item->mutable_last_modification_log()->set_username(rmr.lastModificationLog.username);
    rmr_item->mutable_last_modification_log()->set_host(rmr.lastModificationLog.host);
    rmr_item->mutable_last_modification_log()->set_time(rmr.lastModificationLog.time);
    rmr_item->set_comment(rmr.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
