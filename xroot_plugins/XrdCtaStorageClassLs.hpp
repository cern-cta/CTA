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


namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class StorageClassLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  StorageClassLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_storageClassList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::StorageClass> m_storageClassList;    //!< List of storage classes from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "StorageClassLsStream";    //!< Identifier for log messages
};


StorageClassLsStream::StorageClassLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
  XrdCtaStream(catalogue, scheduler),
  m_storageClassList(catalogue.getStorageClasses())
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "StorageClassLsStream() constructor");
}

int StorageClassLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_storageClassList.empty() && !is_buffer_full; m_storageClassList.pop_front()) {
    Data record;

    auto &sc      = m_storageClassList.front();
    auto  sc_item = record.mutable_scls_item();

    sc_item->set_name(sc.name);
    sc_item->set_nb_copies(sc.nbCopies);
    sc_item->set_vo(sc.vo.name);
    sc_item->mutable_creation_log()->set_username(sc.creationLog.username);
    sc_item->mutable_creation_log()->set_host(sc.creationLog.host);
    sc_item->mutable_creation_log()->set_time(sc.creationLog.time);
    sc_item->mutable_last_modification_log()->set_username(sc.lastModificationLog.username);
    sc_item->mutable_last_modification_log()->set_host(sc.lastModificationLog.host);
    sc_item->mutable_last_modification_log()->set_time(sc.lastModificationLog.time);
    sc_item->set_comment(sc.comment);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
