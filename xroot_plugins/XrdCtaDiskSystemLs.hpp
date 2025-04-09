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

#include "xroot_plugins/XrdCtaStream.hpp"
#include "disk/DiskSystem.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "disksystem ls" command
 */
class DiskSystemLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  DiskSystemLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_diskSystemList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  cta::disk::DiskSystemList m_diskSystemList;             //!< List of disk systems from the catalogue
  const std::string m_instanceName;

  static constexpr const char* const LOG_SUFFIX  = "DiskSystemLsStream";    //!< Identifier for log messages
};


DiskSystemLsStream::DiskSystemLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_diskSystemList(catalogue.DiskSystem()->getAllDiskSystems()),
  m_instanceName(requestMsg.getInstanceName())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DiskSystemLsStream() constructor");
}

  std::string name;
  std::string fileRegexp;
  std::string freeSpaceQueryURL;
  uint64_t refreshInterval;
  uint64_t targetedFreeSpace;
  
  cta::common::dataStructures::EntryLog creationLog;
  cta::common::dataStructures::EntryLog lastModificationLog;
  std::string comment;

int DiskSystemLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_diskSystemList.empty() && !is_buffer_full; m_diskSystemList.pop_front()) {
    Data record;

    auto &ds      = m_diskSystemList.front();
    auto  ds_item = record.mutable_dsls_item();

    fillDiskSystemItem(ds, ds_item, m_instanceName);
    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
