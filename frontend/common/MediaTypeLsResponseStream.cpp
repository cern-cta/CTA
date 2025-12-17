/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MediaTypeLsResponseStream.hpp"

namespace cta::frontend {

MediaTypeLsResponseStream::MediaTypeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler,
                                                     const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_mediaTypes(catalogue.MediaType()->getMediaTypes()) {}

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
  if (mt.primaryDensityCode.has_value()) {
    mt_item->set_primary_density_code(mt.primaryDensityCode.value());
  }
  if (mt.secondaryDensityCode.has_value()) {
    mt_item->set_secondary_density_code(mt.secondaryDensityCode.value());
  }
  if (mt.nbWraps.has_value()) {
    mt_item->set_number_of_wraps(mt.nbWraps.value());
  }
  if (mt.minLPos.has_value()) {
    mt_item->set_min_lpos(mt.minLPos.value());
  }
  if (mt.maxLPos.has_value()) {
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

}  // namespace cta::frontend
