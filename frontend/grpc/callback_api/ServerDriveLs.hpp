#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/DriveLsResponseStream.hpp"

namespace cta::frontend::grpc {

class DriveLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    DriveLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::string& instanceName,
                       cta::log::LogContext lc,
                       const cta::xrd::Request* request);
};

DriveLsWriteReactor::DriveLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                       cta::Scheduler& scheduler,
                                       const std::string& instanceName,
                                       cta::log::LogContext lc,
                                       const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::DRIVE_LS) {
    initializeStream<cta::cmdline::DriveLsResponseStream>(request, catalogue, scheduler, instanceName, lc);
}

} // namespace cta::frontend::grpc