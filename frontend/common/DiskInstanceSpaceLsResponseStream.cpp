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

#include "DiskInstanceSpaceLsResponseStream.hpp"

namespace cta::frontend {

DiskInstanceSpaceLsResponseStream::DiskInstanceSpaceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string& instanceName,
                                                                     const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_diskInstanceSpaces(catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces()) {}

bool DiskInstanceSpaceLsResponseStream::isDone() {
  return m_diskInstanceSpaces.empty();
}

cta::xrd::Data DiskInstanceSpaceLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto dis = m_diskInstanceSpaces.front();
  m_diskInstanceSpaces.pop_front();

  cta::xrd::Data data;
  auto dis_item = data.mutable_disls_item();

  dis_item->set_name(dis.name);
  dis_item->set_instance_name(dis.name);
  dis_item->set_disk_instance(dis.diskInstance);
  dis_item->set_refresh_interval(dis.refreshInterval);
  dis_item->set_free_space_query_url(dis.freeSpaceQueryURL);
  dis_item->set_free_space(dis.freeSpace);
  dis_item->mutable_creation_log()->set_username(dis.creationLog.username);
  dis_item->mutable_creation_log()->set_host(dis.creationLog.host);
  dis_item->mutable_creation_log()->set_time(dis.creationLog.time);
  dis_item->mutable_last_modification_log()->set_username(dis.lastModificationLog.username);
  dis_item->mutable_last_modification_log()->set_host(dis.lastModificationLog.host);
  dis_item->mutable_last_modification_log()->set_time(dis.lastModificationLog.time);
  dis_item->set_comment(dis.comment);

  return data;
}

}  // namespace cta::frontend