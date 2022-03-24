/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>

#include "disk/DiskSystem.hpp"


namespace cta { namespace xrd {

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
  DiskSystemLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

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

  static constexpr const char* const LOG_SUFFIX  = "DiskSystemLsStream";    //!< Identifier for log messages
};


DiskSystemLsStream::DiskSystemLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_diskSystemList(catalogue.getAllDiskSystems())
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

    ds_item->set_name(ds.name);
    ds_item->set_file_regexp(ds.fileRegexp);
    ds_item->set_free_space_query_url(ds.freeSpaceQueryURL);
    ds_item->set_refresh_interval(ds.refreshInterval);
    ds_item->set_targeted_free_space(ds.targetedFreeSpace);
    ds_item->set_sleep_time(ds.sleepTime);
    ds_item->mutable_creation_log()->set_username(ds.creationLog.username);
    ds_item->mutable_creation_log()->set_host(ds.creationLog.host);
    ds_item->mutable_creation_log()->set_time(ds.creationLog.time);
    ds_item->mutable_last_modification_log()->set_username(ds.lastModificationLog.username);
    ds_item->mutable_last_modification_log()->set_host(ds.lastModificationLog.host);
    ds_item->mutable_last_modification_log()->set_time(ds.lastModificationLog.time);
    ds_item->set_comment(ds.comment);
    if (ds.diskInstanceName) {
      ds_item->set_disk_instance(ds.diskInstanceName.value());
    }
    if (ds.diskInstanceSpaceName) {
      ds_item->set_disk_instance_space(ds.diskInstanceSpaceName.value());
    }
    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
