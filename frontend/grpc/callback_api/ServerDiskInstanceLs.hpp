#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/DiskInstanceLsResponseStream.hpp"

namespace cta::frontend::grpc {

class DiskInstanceLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    DiskInstanceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                              cta::Scheduler& scheduler,
                              const std::string& instanceName,
                              const cta::xrd::Request* request);

};

DiskInstanceLsWriteReactor::DiskInstanceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                      cta::Scheduler& scheduler,
                                                      const std::string& instanceName,
                                                      const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::DISKINSTANCE_LS) {
    initializeStream<cta::cmdline::DiskInstanceLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc