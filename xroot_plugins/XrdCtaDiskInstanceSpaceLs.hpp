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
#include "common/dataStructures/DiskInstanceSpace.hpp"

namespace cta { namespace xrd {

/*!
 * Stream object which implements "diskinstancespace ls" command
 */
class DiskInstanceSpaceLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  DiskInstanceSpaceLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_diskInstanceSpaceList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaceList;             //!< List of disk instance spaces from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "DiskInstanceSpaceLsStream";    //!< Identifier for log messages
};


DiskInstanceSpaceLsStream::DiskInstanceSpaceLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_diskInstanceSpaceList(catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DiskInstanceSpaceLsStream() constructor");
}

int DiskInstanceSpaceLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_diskInstanceSpaceList.empty() && !is_buffer_full; m_diskInstanceSpaceList.pop_front()) {
    Data record;

    auto &dis      = m_diskInstanceSpaceList.front();
    auto  dis_item = record.mutable_disls_item();

    dis_item->set_name(dis.name);
    dis_item->set_disk_instance(dis.diskInstance);
    dis_item->set_refresh_interval(dis.refreshInterval);
    dis_item->set_free_space_query_url(dis.freeSpaceQueryURL);
    dis_item->set_free_space(dis.freeSpace);
    dis_item->mutable_creation_log()->set_username(dis.creationLog.username);
    dis_item->mutable_creation_log()->set_host(dis.creationLog.host);
    dis_item->mutable_creation_log()->set_time(dis.creationLog.time);
    dis_item->mutable_last_modification_log()->set_username(dis.lastModificationLog.username);
    dis_item->mutable_last_modification_log()->set_host(dis.lastModificationLog.host);
    dis_item->mutable_last_modification_log()->set_time(dis.lastModificationLog.time);
    dis_item->set_comment(dis.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
