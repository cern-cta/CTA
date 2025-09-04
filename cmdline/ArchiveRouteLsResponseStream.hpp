#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>
#include <common/dataStructures/ArchiveRouteTypeSerDeser.hpp>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class ArchiveRouteLsResponseStream : public CtaAdminResponseStream {
public:
  ArchiveRouteLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const frontend::AdminCmdStream& requestMsg);

  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  std::list<cta::common::dataStructures::ArchiveRoute> m_archiveRoutes;
};

ArchiveRouteLsResponseStream::ArchiveRouteLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()),
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

}  // namespace cta::cmdline