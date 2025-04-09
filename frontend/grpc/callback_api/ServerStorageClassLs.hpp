#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/StorageClassLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class StorageClassLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    StorageClassLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                              cta::Scheduler& scheduler,
                              const std::string& instanceName,
                              const cta::xrd::Request* request);
};

StorageClassLsWriteReactor::StorageClassLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                      cta::Scheduler& scheduler,
                                                      const std::string& instanceName,
                                                      const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::STORAGECLASS_LS) {
    initializeStream<cta::cmdline::StorageClassLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc