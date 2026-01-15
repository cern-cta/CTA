/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ActivityMountRuleLsResponseStream.hpp"

namespace cta::frontend {

ActivityMountRuleLsResponseStream::ActivityMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_activityMountRules(catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules()) {}

bool ActivityMountRuleLsResponseStream::isDone() {
  return m_activityMountRulesIdx >= m_activityMountRules.size();
}

cta::xrd::Data ActivityMountRuleLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto amr = m_activityMountRules[m_activityMountRulesIdx++];

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
