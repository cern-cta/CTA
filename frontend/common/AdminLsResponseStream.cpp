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

#include "AdminLsResponseStream.hpp"

namespace cta::frontend {

AdminLsResponseStream::AdminLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                             cta::Scheduler& scheduler,
                                             const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_adminUsers(catalogue.AdminUser()->getAdminUsers()) {}

bool AdminLsResponseStream::isDone() {
  return m_adminUsers.empty();
}

cta::xrd::Data AdminLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ad = m_adminUsers.front();
  m_adminUsers.pop_front();

  cta::xrd::Data data;
  auto ad_item = data.mutable_adls_item();

  ad_item->set_user(ad.name);
  ad_item->mutable_creation_log()->set_username(ad.creationLog.username);
  ad_item->mutable_creation_log()->set_host(ad.creationLog.host);
  ad_item->mutable_creation_log()->set_time(ad.creationLog.time);
  ad_item->mutable_last_modification_log()->set_username(ad.lastModificationLog.username);
  ad_item->mutable_last_modification_log()->set_host(ad.lastModificationLog.host);
  ad_item->mutable_last_modification_log()->set_time(ad.lastModificationLog.time);
  ad_item->set_comment(ad.comment);
  ad_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::frontend