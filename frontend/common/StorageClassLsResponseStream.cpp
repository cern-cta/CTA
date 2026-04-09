/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  std::optional<std::string> vid = request.getOptional(cta::admin::OptionString::VID);

  if (storageClassName.has_value() && vid.has_value()) {
    throw cta::exception::UserError("Cannot specify both storage class name and VID");
  }

  if (storageClassName.has_value()) {
    m_storageClasses.push_back(m_catalogue.StorageClass()->getStorageClass(storageClassName.value()));
  } else if (vid.has_value()) {
    for (const auto& storageClass : m_catalogue.StorageClass()->getStorageClassesByVid(vid.value())) {
      m_storageClasses.push_back(storageClass);
    }
  } else {
    for (const auto& storageClass : m_catalogue.StorageClass()->getStorageClasses()) {
      m_storageClasses.push_back(storageClass);
    }
  }
}

bool StorageClassLsResponseStream::isDone() {
  return m_storageClassesIdx >= m_storageClasses.size();
}

cta::xrd::Data StorageClassLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto sc = m_storageClasses[m_storageClassesIdx++];

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
