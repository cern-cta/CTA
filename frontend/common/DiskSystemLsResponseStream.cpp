/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskSystemLsResponseStream.hpp"

namespace cta::frontend {

DiskSystemLsResponseStream::DiskSystemLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                       cta::Scheduler& scheduler,
                                                       const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_diskSystems(catalogue.DiskSystem()->getAllDiskSystems()) {}

bool DiskSystemLsResponseStream::isDone() {
  return m_diskSystems.empty();
}

cta::xrd::Data DiskSystemLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ds = m_diskSystems[m_diskSystemsIdx++];

  cta::xrd::Data data;
  auto ds_item = data.mutable_dsls_item();

  ds_item->set_name(ds.name);
  ds_item->set_instance_name(m_instanceName);
  ds_item->set_file_regexp(ds.fileRegexp);
  ds_item->set_disk_instance(ds.diskInstanceSpace.diskInstance);
  ds_item->set_disk_instance_space(ds.diskInstanceSpace.name);
  ds_item->set_targeted_free_space(ds.targetedFreeSpace);
  ds_item->set_sleep_time(ds.sleepTime);
  ds_item->mutable_creation_log()->set_username(ds.creationLog.username);
  ds_item->mutable_creation_log()->set_host(ds.creationLog.host);
  ds_item->mutable_creation_log()->set_time(ds.creationLog.time);
  ds_item->mutable_last_modification_log()->set_username(ds.lastModificationLog.username);
  ds_item->mutable_last_modification_log()->set_host(ds.lastModificationLog.host);
  ds_item->mutable_last_modification_log()->set_time(ds.lastModificationLog.time);
  ds_item->set_comment(ds.comment);

  return data;
}

}  // namespace cta::frontend
