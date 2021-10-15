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
 * Stream object which implements "activitymountrule ls" command
 */
class ActivityMountRuleLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ActivityMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_activityMountRuleList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::RequesterActivityMountRule> m_activityMountRuleList;    //!< List of group mount rules from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "ActivityMountRuleLsStream";               //!< Identifier for log messages
};


ActivityMountRuleLsStream::ActivityMountRuleLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_activityMountRuleList(catalogue.getRequesterActivityMountRules())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ActivityMountRuleLsStream() constructor");
}

int ActivityMountRuleLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_activityMountRuleList.empty() && !is_buffer_full; m_activityMountRuleList.pop_front()) {
    Data record;

    auto &amr      = m_activityMountRuleList.front();
    auto amr_item  = record.mutable_amrls_item();

    amr_item->set_disk_instance(amr.diskInstance);
    amr_item->set_activity_mount_rule(amr.name);
    amr_item->set_mount_policy(amr.mountPolicy);
    amr_item->set_activity_regex(amr.activityRegex);
    amr_item->mutable_creation_log()->set_username(amr.creationLog.username);
    amr_item->mutable_creation_log()->set_host(amr.creationLog.host);
    amr_item->mutable_creation_log()->set_time(amr.creationLog.time);
    amr_item->mutable_last_modification_log()->set_username(amr.lastModificationLog.username);
    amr_item->mutable_last_modification_log()->set_host(amr.lastModificationLog.host);
    amr_item->mutable_last_modification_log()->set_time(amr.lastModificationLog.time);
    amr_item->set_comment(amr.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
