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