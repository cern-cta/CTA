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
class GroupMountRuleLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  GroupMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_groupMountRuleList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::RequesterGroupMountRule> m_groupMountRuleList;    //!< List of group mount rules from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "GroupMountRuleLsStream";               //!< Identifier for log messages
};


GroupMountRuleLsStream::GroupMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_groupMountRuleList(catalogue.getRequesterGroupMountRules())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GroupMountRuleLsStream() constructor");
}

int GroupMountRuleLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_groupMountRuleList.empty() && !is_buffer_full; m_groupMountRuleList.pop_front()) {
    Data record;

    auto &gmr      = m_groupMountRuleList.front();
    auto  gmr_item = record.mutable_gmrls_item();

    gmr_item->set_disk_instance(gmr.diskInstance);
    gmr_item->set_group_mount_rule(gmr.name);
    gmr_item->set_mount_policy(gmr.mountPolicy);
    gmr_item->mutable_creation_log()->set_username(gmr.creationLog.username);
    gmr_item->mutable_creation_log()->set_host(gmr.creationLog.host);
    gmr_item->mutable_creation_log()->set_time(gmr.creationLog.time);
    gmr_item->mutable_last_modification_log()->set_username(gmr.lastModificationLog.username);
    gmr_item->mutable_last_modification_log()->set_host(gmr.lastModificationLog.host);
    gmr_item->mutable_last_modification_log()->set_time(gmr.lastModificationLog.time);
    gmr_item->set_comment(gmr.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
