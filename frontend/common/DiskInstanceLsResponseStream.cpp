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

#include "DiskInstanceLsResponseStream.hpp"

namespace cta::frontend {

DiskInstanceLsResponseStream::DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const std::string& instanceName,
                                                           const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_diskInstances(catalogue.DiskInstance()->getAllDiskInstances()) {}

bool DiskInstanceLsResponseStream::isDone() {
  return m_diskInstances.empty();
}

cta::xrd::Data DiskInstanceLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto di = m_diskInstances.front();
  m_diskInstances.pop_front();

  cta::xrd::Data data;
  auto di_item = data.mutable_dils_item();

  di_item->set_name(di.name);
  di_item->set_instance_name(m_instanceName);
  di_item->mutable_creation_log()->set_username(di.creationLog.username);
  di_item->mutable_creation_log()->set_host(di.creationLog.host);
  di_item->mutable_creation_log()->set_time(di.creationLog.time);
  di_item->mutable_last_modification_log()->set_username(di.lastModificationLog.username);
  di_item->mutable_last_modification_log()->set_host(di.lastModificationLog.host);
  di_item->mutable_last_modification_log()->set_time(di.lastModificationLog.time);
  di_item->set_comment(di.comment);

  return data;
}

}  // namespace cta::frontend