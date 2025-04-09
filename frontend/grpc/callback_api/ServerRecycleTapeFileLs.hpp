#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/RecycleTapeFileLsResponseStream.hpp"

namespace cta::frontend::grpc {

class RecycleTapeFileLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        RecycleTapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

RecycleTapeFileLsWriteReactor::RecycleTapeFileLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::RECYLETAPEFILE_LS) {
    initializeStream<cta::cmdline::RecycleTapeFileLsResponseStream>(request, catalogue, scheduler, instanceName);
}

} // namespace cta::frontend::grpc