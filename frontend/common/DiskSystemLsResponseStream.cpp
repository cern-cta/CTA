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

  const auto ds = m_diskSystems.front();
  m_diskSystems.pop_front();

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