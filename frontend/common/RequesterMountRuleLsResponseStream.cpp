/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RequesterMountRuleLsResponseStream.hpp"

namespace cta::frontend {

RequesterMountRuleLsResponseStream::RequesterMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                       cta::Scheduler& scheduler,
                                                                       const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_requesterMountRules(catalogue.RequesterMountRule()->getRequesterMountRules()) {}

bool RequesterMountRuleLsResponseStream::isDone() {
  return m_requesterMountRules.empty();
}

cta::xrd::Data RequesterMountRuleLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto rmr = m_requesterMountRules.front();
  m_requesterMountRules.pop_front();

  cta::xrd::Data data;
  auto rmr_item = data.mutable_rmrls_item();

  rmr_item->set_instance_name(m_instanceName);
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

  return data;
}

}  // namespace cta::frontend
