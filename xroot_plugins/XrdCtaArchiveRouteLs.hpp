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

namespace cta {
namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class ArchiveRouteLsStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ArchiveRouteLsStream(const frontend::AdminCmdStream& requestMsg,
                       cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const { return m_archiveRouteList.empty(); }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf);

  std::list<cta::common::dataStructures::ArchiveRoute>
    m_archiveRouteList;  //!< List of archive routes from the catalogue

  static constexpr const char* const LOG_SUFFIX = "ArchiveRouteLsStream";  //!< Identifier for log messages
};

ArchiveRouteLsStream::ArchiveRouteLsStream(const frontend::AdminCmdStream& requestMsg,
                                           cta::catalogue::Catalogue& catalogue,
                                           cta::Scheduler& scheduler) :
XrdCtaStream(catalogue, scheduler),
m_archiveRouteList(catalogue.ArchiveRoute()->getArchiveRoutes()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ArchiveRouteLsStream() constructor");
}

int ArchiveRouteLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  for (bool is_buffer_full = false; !m_archiveRouteList.empty() && !is_buffer_full; m_archiveRouteList.pop_front()) {
    Data record;

    auto& ar = m_archiveRouteList.front();
    auto ar_item = record.mutable_arls_item();

    ar_item->set_storage_class(ar.storageClassName);
    ar_item->set_copy_number(ar.copyNb);
    ar_item->set_tapepool(ar.tapePoolName);
    ar_item->mutable_creation_log()->set_username(ar.creationLog.username);
    ar_item->mutable_creation_log()->set_host(ar.creationLog.host);
    ar_item->mutable_creation_log()->set_time(ar.creationLog.time);
    ar_item->mutable_last_modification_log()->set_username(ar.lastModificationLog.username);
    ar_item->mutable_last_modification_log()->set_host(ar.lastModificationLog.host);
    ar_item->mutable_last_modification_log()->set_time(ar.lastModificationLog.time);
    ar_item->set_comment(ar.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace xrd
}  // namespace cta
