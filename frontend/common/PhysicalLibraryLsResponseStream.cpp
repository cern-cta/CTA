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

#include "PhysicalLibraryLsResponseStream.hpp"

namespace cta::frontend {

PhysicalLibraryLsResponseStream::PhysicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                 cta::Scheduler& scheduler,
                                                                 const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_physicalLibraries(catalogue.PhysicalLibrary()->getPhysicalLibraries()) {}

bool PhysicalLibraryLsResponseStream::isDone() {
  return m_physicalLibraries.empty();
}

cta::xrd::Data PhysicalLibraryLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto pl = m_physicalLibraries.front();
  m_physicalLibraries.pop_front();

  cta::xrd::Data data;
  auto pl_item = data.mutable_plls_item();

  pl_item->set_name(pl.name);
  pl_item->set_instance_name(m_instanceName);
  pl_item->set_manufacturer(pl.manufacturer);
  pl_item->set_model(pl.model);

  if (pl.type.has_value()) {
    pl_item->set_type(pl.type.value());
  }
  if (pl.guiUrl.has_value()) {
    pl_item->set_gui_url(pl.guiUrl.value());
  }
  if (pl.webcamUrl.has_value()) {
    pl_item->set_webcam_url(pl.webcamUrl.value());
  }
  if (pl.location.has_value()) {
    pl_item->set_location(pl.location.value());
  }
  if (pl.nbAvailableCartridgeSlots.has_value()) {
    pl_item->set_nb_available_cartridge_slots(pl.nbAvailableCartridgeSlots.value());
  }
  if (pl.comment.has_value()) {
    pl_item->set_comment(pl.comment.value());
  }
  if (pl.disabledReason.has_value()) {
    pl_item->set_disabled_reason(pl.disabledReason.value());
  }

  pl_item->set_nb_physical_cartridge_slots(pl.nbPhysicalCartridgeSlots);
  pl_item->set_nb_physical_drive_slots(pl.nbPhysicalDriveSlots);
  pl_item->mutable_creation_log()->set_username(pl.creationLog.username);
  pl_item->mutable_creation_log()->set_host(pl.creationLog.host);
  pl_item->mutable_creation_log()->set_time(pl.creationLog.time);
  pl_item->mutable_last_modification_log()->set_username(pl.lastModificationLog.username);
  pl_item->mutable_last_modification_log()->set_host(pl.lastModificationLog.host);
  pl_item->mutable_last_modification_log()->set_time(pl.lastModificationLog.time);
  pl_item->set_is_disabled(pl.isDisabled);

  return data;
}

}  // namespace cta::frontend
