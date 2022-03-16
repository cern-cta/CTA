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
 * Stream object which implements "virtualorganization ls" command
 */
class VirtualOrganizationLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  VirtualOrganizationLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_virtualOrganizationList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::VirtualOrganization> m_virtualOrganizationList;             //!< List of virtual organizations from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "VirtualOrganizationLsStream";    //!< Identifier for log messages
};


VirtualOrganizationLsStream::VirtualOrganizationLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_virtualOrganizationList(catalogue.getVirtualOrganizations())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "VirtualOrganizationLsStream() constructor");
}

int VirtualOrganizationLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_virtualOrganizationList.empty() && !is_buffer_full; m_virtualOrganizationList.pop_front()) {
    Data record;

    auto &vo      = m_virtualOrganizationList.front();
    auto  vo_item = record.mutable_vols_item();

    vo_item->set_name(vo.name);
    vo_item->set_read_max_drives(vo.readMaxDrives);
    vo_item->set_write_max_drives(vo.writeMaxDrives);
    vo_item->set_max_file_size(vo.maxFileSize);
    vo_item->mutable_creation_log()->set_username(vo.creationLog.username);
    vo_item->mutable_creation_log()->set_host(vo.creationLog.host);
    vo_item->mutable_creation_log()->set_time(vo.creationLog.time);
    vo_item->mutable_last_modification_log()->set_username(vo.lastModificationLog.username);
    vo_item->mutable_last_modification_log()->set_host(vo.lastModificationLog.host);
    vo_item->mutable_last_modification_log()->set_time(vo.lastModificationLog.time);
    vo_item->set_comment(vo.comment);

    if (vo.diskInstanceName) {
      vo_item->set_diskinstance(vo.diskInstanceName.value());
    }

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
