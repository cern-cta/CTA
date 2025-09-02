#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class MediaTypeLsResponseStream : public CtaAdminResponseStream {
public:
  MediaTypeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler,
                            const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;
  void init(const admin::AdminCmd& admincmd) override;

private:
  std::list<cta::catalogue::MediaTypeWithLogs> m_mediaTypes;
};

MediaTypeLsResponseStream::MediaTypeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler,
                                                     const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {}

void MediaTypeLsResponseStream::init(const admin::AdminCmd& admincmd) {
  // Media type ls doesn't have specific search criteria - it just gets all media types
  m_mediaTypes = m_catalogue.MediaType()->getMediaTypes();
}

bool MediaTypeLsResponseStream::isDone() {
  return m_mediaTypes.empty();
}

cta::xrd::Data MediaTypeLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto mt = m_mediaTypes.front();
  m_mediaTypes.pop_front();

  cta::xrd::Data data;
  auto mt_item = data.mutable_mtls_item();

  mt_item->set_name(mt.name);
  mt_item->set_instance_name(m_instanceName);
  mt_item->set_cartridge(mt.cartridge);
  mt_item->set_capacity(mt.capacityInBytes);
  if (mt.primaryDensityCode) {
    mt_item->set_primary_density_code(mt.primaryDensityCode.value());
  }
  if (mt.secondaryDensityCode) {
    mt_item->set_secondary_density_code(mt.secondaryDensityCode.value());
  }
  if (mt.nbWraps) {
    mt_item->set_number_of_wraps(mt.nbWraps.value());
  }
  if (mt.minLPos) {
    mt_item->set_min_lpos(mt.minLPos.value());
  }
  if (mt.maxLPos) {
    mt_item->set_max_lpos(mt.maxLPos.value());
  }
  mt_item->set_comment(mt.comment);
  mt_item->mutable_creation_log()->set_username(mt.creationLog.username);
  mt_item->mutable_creation_log()->set_host(mt.creationLog.host);
  mt_item->mutable_creation_log()->set_time(mt.creationLog.time);
  mt_item->mutable_last_modification_log()->set_username(mt.lastModificationLog.username);
  mt_item->mutable_last_modification_log()->set_host(mt.lastModificationLog.host);
  mt_item->mutable_last_modification_log()->set_time(mt.lastModificationLog.time);

  return data;
}

}  // namespace cta::cmdline