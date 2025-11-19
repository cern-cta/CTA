/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "ActivityMountRuleLsResponseStream.hpp"

namespace cta::frontend {

ActivityMountRuleLsResponseStream::ActivityMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string& instanceName,
                                                                     const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_activityMountRules(catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules()) {}

bool ActivityMountRuleLsResponseStream::isDone() {
  return m_activityMountRules.empty();
}

cta::xrd::Data ActivityMountRuleLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto amr = m_activityMountRules.front();
  m_activityMountRules.pop_front();

  cta::xrd::Data data;
  auto amr_item = data.mutable_amrls_item();

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
  amr_item->set_comment(amr.comment), amr_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::frontend