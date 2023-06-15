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

#include "xroot_plugins/XrdCtaStream.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"

namespace cta {
namespace xrd {

/*!
 * Stream object which implements "activitymountrule ls" command
 */
class ActivityMountRuleLsStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ActivityMountRuleLsStream(const frontend::AdminCmdStream& requestMsg,
                            cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const { return m_activityMountRuleList.empty(); }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf);

  std::list<cta::common::dataStructures::RequesterActivityMountRule>
    m_activityMountRuleList;  //!< List of group mount rules from the catalogue

  static constexpr const char* const LOG_SUFFIX = "ActivityMountRuleLsStream";  //!< Identifier for log messages
};

ActivityMountRuleLsStream::ActivityMountRuleLsStream(const frontend::AdminCmdStream& requestMsg,
                                                     cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler) :
XrdCtaStream(catalogue, scheduler),
m_activityMountRuleList(catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ActivityMountRuleLsStream() constructor");
}

int ActivityMountRuleLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  for (bool is_buffer_full = false; !m_activityMountRuleList.empty() && !is_buffer_full;
       m_activityMountRuleList.pop_front()) {
    Data record;

    auto& amr = m_activityMountRuleList.front();
    auto amr_item = record.mutable_amrls_item();

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

}  // namespace xrd
}  // namespace cta
