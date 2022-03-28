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
