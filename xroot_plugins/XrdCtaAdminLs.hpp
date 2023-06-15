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

#include "common/dataStructures/AdminUser.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta {
namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class AdminLsStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  AdminLsStream(const frontend::AdminCmdStream& requestMsg,
                cta::catalogue::Catalogue& catalogue,
                cta::Scheduler& scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const { return m_adminList.empty(); }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf);

  std::list<cta::common::dataStructures::AdminUser> m_adminList;  //!< List of admin users from the catalogue

  static constexpr const char* const LOG_SUFFIX = "AdminLsStream";  //!< Identifier for log messages
};

AdminLsStream::AdminLsStream(const frontend::AdminCmdStream& requestMsg,
                             cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler) :
XrdCtaStream(catalogue, scheduler),
m_adminList(catalogue.AdminUser()->getAdminUsers()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "AdminLsStream() constructor");
}

int AdminLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  for (bool is_buffer_full = false; !m_adminList.empty() && !is_buffer_full; m_adminList.pop_front()) {
    Data record;

    auto& ad = m_adminList.front();
    auto ad_item = record.mutable_adls_item();

    ad_item->set_user(ad.name);
    ad_item->mutable_creation_log()->set_username(ad.creationLog.username);
    ad_item->mutable_creation_log()->set_host(ad.creationLog.host);
    ad_item->mutable_creation_log()->set_time(ad.creationLog.time);
    ad_item->mutable_last_modification_log()->set_username(ad.lastModificationLog.username);
    ad_item->mutable_last_modification_log()->set_host(ad.lastModificationLog.host);
    ad_item->mutable_last_modification_log()->set_time(ad.lastModificationLog.time);
    ad_item->set_comment(ad.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace xrd
}  // namespace cta
