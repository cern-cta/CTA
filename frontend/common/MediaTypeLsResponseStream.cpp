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

#include "MediaTypeLsResponseStream.hpp"

namespace cta::frontend {

MediaTypeLsResponseStream::MediaTypeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler,
                                                     const std::string& instanceName,
                                                     const admin::AdminCmd& adminCmd)
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
