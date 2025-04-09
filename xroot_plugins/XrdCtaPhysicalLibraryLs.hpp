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
#include "cmdline/admin_common/DataItemMessageFill.hpp"

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
  const std::string m_instanceName;

  static constexpr const char* const LOG_SUFFIX  = "PhysicalLibraryLsStream";      //!< Identifier for log messages
};


PhysicalLibraryLsStream::PhysicalLibraryLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_physicalLibraryList(catalogue.PhysicalLibrary()->getPhysicalLibraries()),
  m_instanceName(requestMsg.getInstanceName()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "PhysicalLibraryLsStream() constructor");
}

int PhysicalLibraryLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for (bool is_buffer_full = false; !m_physicalLibraryList.empty()
    && !is_buffer_full; m_physicalLibraryList.pop_front()) {
    Data record;

    auto &pl     = m_physicalLibraryList.front();
    auto pl_item = record.mutable_plls_item();

    fillPhysicalLibraryItem(pl, pl_item, m_instanceName);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
