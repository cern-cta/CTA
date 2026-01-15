/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  return m_archiveRoutesIdx >= m_archiveRoutes.size();
}

cta::xrd::Data ArchiveRouteLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ar = m_archiveRoutes[m_archiveRoutesIdx++];

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
