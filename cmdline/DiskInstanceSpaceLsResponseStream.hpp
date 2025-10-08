/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include <list>

#include "cta_frontend.pb.h"
#include "cta_admin.pb.h"

namespace cta::cmdline {

class DiskInstanceSpaceLsResponseStream : public CtaAdminResponseStream {
public:
  DiskInstanceSpaceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                    cta::Scheduler& scheduler,
                                    const std::string instanceName,
                                    const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaces;
};

DiskInstanceSpaceLsResponseStream::DiskInstanceSpaceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string instanceName,
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

}  // namespace cta::cmdline
