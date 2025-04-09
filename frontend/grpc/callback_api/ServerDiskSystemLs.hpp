#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/DiskSystemLsResponseStream.hpp"

namespace cta::frontend::grpc {

class DiskSystemLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        DiskSystemLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

DiskSystemLsWriteReactor::DiskSystemLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::DISKSYSTEM_LS) {
    initializeStream<cta::cmdline::DiskSystemLsResponseStream>(request, catalogue, scheduler, instanceName);
    
}

} // namespace cta::frontend::grpc