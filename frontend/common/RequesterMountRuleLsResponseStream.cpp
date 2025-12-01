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