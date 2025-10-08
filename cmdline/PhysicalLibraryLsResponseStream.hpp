/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include <list>

#include "cta_frontend.pb.h"
#include "cta_admin.pb.h"

namespace cta::cmdline {

class PhysicalLibraryLsResponseStream : public CtaAdminResponseStream {
public:
  PhysicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                  cta::Scheduler& scheduler,
                                  const std::string instanceName,
                                  const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::PhysicalLibrary> m_physicalLibraries;
};

PhysicalLibraryLsResponseStream::PhysicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                 cta::Scheduler& scheduler,
                                                                 const std::string instanceName,
                                                                 const admin::AdminCmd& adminCmd)
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

  if (pl.type) {
    pl_item->set_type(pl.type.value());
  }
  if (pl.guiUrl) {
    pl_item->set_gui_url(pl.guiUrl.value());
  }
  if (pl.webcamUrl) {
    pl_item->set_webcam_url(pl.webcamUrl.value());
  }
  if (pl.location) {
    pl_item->set_location(pl.location.value());
  }
  if (pl.nbAvailableCartridgeSlots) {
    pl_item->set_nb_available_cartridge_slots(pl.nbAvailableCartridgeSlots.value());
  }
  if (pl.comment) {
    pl_item->set_comment(pl.comment.value());
  }
  if (pl.disabledReason) {
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

}  // namespace cta::cmdline
