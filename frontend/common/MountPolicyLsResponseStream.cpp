/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountPolicyLsResponseStream.hpp"

namespace cta::frontend {

MountPolicyLsResponseStream::MountPolicyLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                         cta::Scheduler& scheduler,
                                                         const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_mountPolicies(catalogue.MountPolicy()->getMountPolicies()) {}

bool MountPolicyLsResponseStream::isDone() {
  return m_mountPolicies.empty();
}

cta::xrd::Data MountPolicyLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto mp = m_mountPolicies.front();
  m_mountPolicies.pop_front();

  cta::xrd::Data data;
  auto mpItem = data.mutable_mpls_item();

  mpItem->set_name(mp.name);
  mpItem->set_instance_name(m_instanceName);
  mpItem->set_archive_priority(mp.archivePriority);
  mpItem->set_archive_min_request_age(mp.archiveMinRequestAge);
  mpItem->set_retrieve_priority(mp.retrievePriority);
  mpItem->set_retrieve_min_request_age(mp.retrieveMinRequestAge);
  mpItem->mutable_creation_log()->set_username(mp.creationLog.username);
  mpItem->mutable_creation_log()->set_host(mp.creationLog.host);
  mpItem->mutable_creation_log()->set_time(mp.creationLog.time);
  mpItem->mutable_last_modification_log()->set_username(mp.lastModificationLog.username);
  mpItem->mutable_last_modification_log()->set_host(mp.lastModificationLog.host);
  mpItem->mutable_last_modification_log()->set_time(mp.lastModificationLog.time);
  mpItem->set_comment(mp.comment);

  return data;
}

}  // namespace cta::frontend
