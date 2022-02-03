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

#include "common/dataStructures/DiskInstance.hpp"


namespace cta { namespace xrd {

/*!
 * Stream object which implements "disksystem ls" command
 */
class DiskInstanceLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  DiskInstanceLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_diskInstanceList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<common::dataStructures::DiskInstance> m_diskInstanceList;             //!< List of disk instances from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "DiskInstanceLsStream";    //!< Identifier for log messages
};


DiskInstanceLsStream::DiskInstanceLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_diskInstanceList(catalogue.getAllDiskInstances())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DiskInstanceLsStream() constructor");
}

int DiskInstanceLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_diskInstanceList.empty() && !is_buffer_full; m_diskInstanceList.pop_front()) {
    Data record;

    auto &di      = m_diskInstanceList.front();
    auto  di_item = record.mutable_dils_item();

    di_item->set_name(di.name);
    di_item->mutable_creation_log()->set_username(di.creationLog.username);
    di_item->mutable_creation_log()->set_host(di.creationLog.host);
    di_item->mutable_creation_log()->set_time(di.creationLog.time);
    di_item->mutable_last_modification_log()->set_username(di.lastModificationLog.username);
    di_item->mutable_last_modification_log()->set_host(di.lastModificationLog.host);
    di_item->mutable_last_modification_log()->set_time(di.lastModificationLog.time);
    di_item->set_comment(di.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
