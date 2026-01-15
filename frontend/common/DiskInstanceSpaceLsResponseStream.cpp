/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskInstanceSpaceLsResponseStream.hpp"

namespace cta::frontend {

DiskInstanceSpaceLsResponseStream::DiskInstanceSpaceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_diskInstanceSpaces(catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces()) {}

bool DiskInstanceSpaceLsResponseStream::isDone() {
  return m_diskInstanceSpacesIdx >= m_diskInstanceSpaces.size();
}

cta::xrd::Data DiskInstanceSpaceLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto dis = m_diskInstanceSpaces[m_diskInstanceSpacesIdx++];

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
