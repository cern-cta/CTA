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

#include "ArchiveRouteLsResponseStream.hpp"

#include "common/dataStructures/ArchiveRouteTypeSerDeser.hpp"

namespace cta::frontend {

ArchiveRouteLsResponseStream::ArchiveRouteLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_archiveRoutes(catalogue.ArchiveRoute()->getArchiveRoutes()) {}

bool ArchiveRouteLsResponseStream::isDone() {
  return m_archiveRoutes.empty();
}

cta::xrd::Data ArchiveRouteLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ar = m_archiveRoutes.front();
  m_archiveRoutes.pop_front();

  cta::xrd::Data data;
  auto ar_item = data.mutable_arls_item();

  ar_item->set_storage_class(ar.storageClassName);
  ar_item->set_copy_number(ar.copyNb);
  ar_item->set_archive_route_type(cta::admin::ArchiveRouteTypeFormatToProtobuf(ar.type));
  ar_item->set_tapepool(ar.tapePoolName);
  ar_item->mutable_creation_log()->set_username(ar.creationLog.username);
  ar_item->mutable_creation_log()->set_host(ar.creationLog.host);
  ar_item->mutable_creation_log()->set_time(ar.creationLog.time);
  ar_item->mutable_last_modification_log()->set_username(ar.lastModificationLog.username);
  ar_item->mutable_last_modification_log()->set_host(ar.lastModificationLog.host);
  ar_item->mutable_last_modification_log()->set_time(ar.lastModificationLog.time);
  ar_item->set_comment(ar.comment);
  ar_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::frontend