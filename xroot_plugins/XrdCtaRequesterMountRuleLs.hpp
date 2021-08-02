/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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
  m_requesterMountRuleList(catalogue.getRequesterMountRules())
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
