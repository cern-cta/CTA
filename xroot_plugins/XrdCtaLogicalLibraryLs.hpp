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


namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class LogicalLibraryLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  LogicalLibraryLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_logicalLibraryList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::LogicalLibrary> m_logicalLibraryList;    //!< List of logical libraries from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "LogicalLibraryLsStream";      //!< Identifier for log messages
};


LogicalLibraryLsStream::LogicalLibraryLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_logicalLibraryList(catalogue.getLogicalLibraries())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "LogicalLibraryLsStream() constructor");
}

int LogicalLibraryLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_logicalLibraryList.empty() && !is_buffer_full; m_logicalLibraryList.pop_front()) {
    Data record;

    auto &ll      = m_logicalLibraryList.front();
    auto  ll_item = record.mutable_llls_item();

    ll_item->set_name(ll.name);
    ll_item->set_is_disabled(ll.isDisabled);
    ll_item->mutable_creation_log()->set_username(ll.creationLog.username);
    ll_item->mutable_creation_log()->set_host(ll.creationLog.host);
    ll_item->mutable_creation_log()->set_time(ll.creationLog.time);
    ll_item->mutable_last_modification_log()->set_username(ll.lastModificationLog.username);
    ll_item->mutable_last_modification_log()->set_host(ll.lastModificationLog.host);
    ll_item->mutable_last_modification_log()->set_time(ll.lastModificationLog.time);
    ll_item->set_comment(ll.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
