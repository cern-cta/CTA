/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "StorageClassLsResponseStream.hpp"

#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

StorageClassLsResponseStream::StorageClassLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const std::string& instanceName,
                                                           const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {
  cta::frontend::AdminCmdOptions request(adminCmd);

  std::optional<std::string> storageClassName = request.getOptional(cta::admin::OptionString::STORAGE_CLASS);

  if (storageClassName.has_value()) {
    m_storageClasses.push_back(m_catalogue.StorageClass()->getStorageClass(storageClassName.value()));
  } else {
    for (const auto& storageClass : m_catalogue.StorageClass()->getStorageClasses()) {
      m_storageClasses.push_back(storageClass);
    }
  }
}

bool StorageClassLsResponseStream::isDone() {
  return m_storageClasses.empty();
}

cta::xrd::Data StorageClassLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto sc = m_storageClasses.front();
  m_storageClasses.pop_front();

  cta::xrd::Data data;
  auto scItem = data.mutable_scls_item();

  scItem->set_name(sc.name);
  scItem->set_nb_copies(sc.nbCopies);
  scItem->set_vo(sc.vo.name);
  scItem->mutable_creation_log()->set_username(sc.creationLog.username);
  scItem->mutable_creation_log()->set_host(sc.creationLog.host);
  scItem->mutable_creation_log()->set_time(sc.creationLog.time);
  scItem->mutable_last_modification_log()->set_username(sc.lastModificationLog.username);
  scItem->mutable_last_modification_log()->set_host(sc.lastModificationLog.host);
  scItem->mutable_last_modification_log()->set_time(sc.lastModificationLog.time);
  scItem->set_comment(sc.comment);
  scItem->set_instance_name(m_instanceName);
  return data;
}

}  // namespace cta::frontend