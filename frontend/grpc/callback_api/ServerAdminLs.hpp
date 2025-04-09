#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/AdminLsResponseStream.hpp"

namespace cta::frontend::grpc {

class AdminLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        AdminLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

AdminLsWriteReactor::AdminLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::ADMIN_LS) {
    initializeStream<cta::cmdline::AdminLsResponseStream>(request, catalogue, scheduler, instanceName);
}

} // namespace cta::frontend::grpc