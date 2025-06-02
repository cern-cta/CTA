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

#include <optional>
#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class StorageClassLsStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  StorageClassLsStream(const frontend::AdminCmdStream& requestMsg,
                       cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::optional<std::string> storageClassName);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const { return m_storageClassList.empty(); }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf);

  std::list<cta::common::dataStructures::StorageClass>
    m_storageClassList;  //!< List of storage classes from the catalogue

  std::optional<std::string> m_storageClassName;
  const std::string m_instanceName;

  static constexpr const char* const LOG_SUFFIX = "StorageClassLsStream";  //!< Identifier for log messages
};

StorageClassLsStream::StorageClassLsStream(const frontend::AdminCmdStream& requestMsg,
                                           cta::catalogue::Catalogue& catalogue,
                                           cta::Scheduler& scheduler,
                                           const std::optional<std::string> storageClassName)
    : XrdCtaStream(catalogue, scheduler),
      m_storageClassName(storageClassName),
      m_instanceName(requestMsg.getInstanceName()) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "StorageClassLsStream() constructor");

  if (m_storageClassName) {
    m_storageClassList.push_back(m_catalogue.StorageClass()->getStorageClass(m_storageClassName.value()));
  } else {
    for (const auto& storageClass : m_catalogue.StorageClass()->getStorageClasses()) {
      m_storageClassList.push_back(storageClass);
    }
  }
}

int StorageClassLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  for (bool is_buffer_full = false; !m_storageClassList.empty() && !is_buffer_full; m_storageClassList.pop_front()) {
    Data record;

    auto& sc = m_storageClassList.front();
    auto sc_item = record.mutable_scls_item();

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
    sc_item->set_instance_name(m_instanceName);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace cta::xrd
