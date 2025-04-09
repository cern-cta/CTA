#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/RepackLsResponseStream.hpp"

namespace cta::frontend::grpc {

class RepackLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

RepackLsWriteReactor::RepackLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::REPACK_LS) {
    initializeStream<cta::cmdline::RepackLsResponseStream>(request, catalogue, scheduler, instanceName);
    
}

} // namespace cta::frontend::grpc