#pragma once

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "TemplateAdminCmdStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class StorageClassLsWriteReactor : public TemplateAdminCmdStream<common::dataStructures::StorageClass,
                                                                 cta::admin::StorageClassLsItem,
                                                                 decltype(&fillStorageClassItem)> {
public:
  StorageClassLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler,
                             const std::string& instanceName,
                             const cta::xrd::Request* request)
      : TemplateAdminCmdStream<common::dataStructures::StorageClass,
                               cta::admin::StorageClassLsItem,
                               decltype(&fillStorageClassItem)>(catalogue,
                                                                scheduler,
                                                                instanceName,
                                                                buildStorageClassList(catalogue, request),
                                                                cta::admin::HeaderType::STORAGECLASS_LS,
                                                                &fillStorageClassItem) {}

protected:
  cta::admin::StorageClassLsItem* getMessageField(cta::xrd::Data* data) override { return data->mutable_scls_item(); }

private:
  static std::list<common::dataStructures::StorageClass> buildStorageClassList(cta::catalogue::Catalogue& catalogue,
                                                                               const cta::xrd::Request* request) {
    request::RequestMessage requestMsg(*request);
    std::optional<std::string> storageClassName = requestMsg.getOptional(cta::admin::OptionString::STORAGE_CLASS);

    std::list<common::dataStructures::StorageClass> storageClassList;

    if (storageClassName.has_value()) {
        storageClassList.push_back(catalogue.StorageClass()->getStorageClass(storageClassName.value()));
    } else {
        for (const auto& storageClass : catalogue.StorageClass()->getStorageClasses()) {
            storageClassList.push_back(storageClass);
        }
    }

        return storageClassList;
    }
};

} // namespace cta::frontend::grpc