/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#pragma once

#include <list>

#include "xroot_plugins/XrdCtaStream.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class PhysicalLibraryLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   * @param[in]    disabled      Physical Library disable status
   */
  PhysicalLibraryLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  bool isDone() const override {
    return m_physicalLibraryList.empty();
  }

  /*!
   * Fill the buffer
   */
  int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) override;

  // List of physical libraries from the catalogue
  std::list<cta::common::dataStructures::PhysicalLibrary> m_physicalLibraryList;
  const std::optional<bool> m_disabled;

  static constexpr const char* const LOG_SUFFIX  = "PhysicalLibraryLsStream";      //!< Identifier for log messages
};


PhysicalLibraryLsStream::PhysicalLibraryLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_physicalLibraryList(catalogue.PhysicalLibrary()->getPhysicalLibraries()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "PhysicalLibraryLsStream() constructor");
}

int PhysicalLibraryLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for (bool is_buffer_full = false; !m_physicalLibraryList.empty()
    && !is_buffer_full; m_physicalLibraryList.pop_front()) {
    Data record;

    auto &pl     = m_physicalLibraryList.front();
    auto pl_item = record.mutable_plls_item();

    pl_item->set_name(pl.name);
    pl_item->set_manufacturer(pl.manufacturer);
    pl_item->set_model(pl.model);

    if (pl.type)                      { pl_item->set_type(pl.type.value()); }
    if (pl.guiUrl)                    { pl_item->set_gui_url(pl.guiUrl.value()); }
    if (pl.webcamUrl)                 { pl_item->set_webcam_url(pl.webcamUrl.value()); }
    if (pl.location)                  { pl_item->set_location(pl.location.value()); }
    if (pl.nbAvailableCartridgeSlots) { pl_item->set_nb_available_cartridge_slots(pl.nbAvailableCartridgeSlots.value()); }
    if (pl.comment)                   { pl_item->set_comment(pl.comment.value()); }
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

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
