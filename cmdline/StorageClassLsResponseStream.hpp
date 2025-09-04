#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class StorageClassLsResponseStream : public CtaAdminResponseStream {
public:
  StorageClassLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const frontend::AdminCmdStream& requestMsg);

  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  std::list<cta::common::dataStructures::StorageClass> m_storageClasses;
};

StorageClassLsResponseStream::StorageClassLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                           cta::Scheduler& scheduler,
                                                           const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()) {

        cta::frontend::AdminCmdOptions request(requestMsg.getAdminCmd());

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

}  // namespace cta::cmdline