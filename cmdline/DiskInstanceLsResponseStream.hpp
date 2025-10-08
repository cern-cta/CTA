/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include <list>

#include "cta_frontend.pb.h"
#include "cta_admin.pb.h"

namespace cta::cmdline {

class DiskInstanceLsResponseStream : public CtaAdminResponseStream {
public:
  DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const std::string instanceName,
                               const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::DiskInstance> m_diskInstances;  //!< List of disk instances from the catalogue
};

DiskInstanceLsResponseStream::DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const std::string instanceName,
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

}  // namespace cta::cmdline
