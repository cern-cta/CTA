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

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class MountPolicyLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  MountPolicyLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_mountPolicyList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::MountPolicy> m_mountPolicyList;     //!< List of mount policies from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "MountPolicyLsStream";    //!< Identifier for log messages
};


MountPolicyLsStream::MountPolicyLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_mountPolicyList(catalogue.MountPolicy()->getMountPolicies())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "MountPolicyLsStream() constructor");
}

int MountPolicyLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_mountPolicyList.empty() && !is_buffer_full; m_mountPolicyList.pop_front()) {
    Data record;

    auto &mp      = m_mountPolicyList.front();
    auto  mp_item = record.mutable_mpls_item();

    mp_item->set_name(mp.name);
    mp_item->set_archive_priority(mp.archivePriority);
    mp_item->set_archive_min_request_age(mp.archiveMinRequestAge);
    mp_item->set_retrieve_priority(mp.retrievePriority);
    mp_item->set_retrieve_min_request_age(mp.retrieveMinRequestAge);
    mp_item->mutable_creation_log()->set_username(mp.creationLog.username);
    mp_item->mutable_creation_log()->set_host(mp.creationLog.host);
    mp_item->mutable_creation_log()->set_time(mp.creationLog.time);
    mp_item->mutable_last_modification_log()->set_username(mp.lastModificationLog.username);
    mp_item->mutable_last_modification_log()->set_host(mp.lastModificationLog.host);
    mp_item->mutable_last_modification_log()->set_time(mp.lastModificationLog.time);
    mp_item->set_comment(mp.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
