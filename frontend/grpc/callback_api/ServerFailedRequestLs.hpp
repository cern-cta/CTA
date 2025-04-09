#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/FailedRequestLsResponseStream.hpp"

namespace cta::frontend::grpc {

class FailedRequestLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        FailedRequestLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, SchedulerDatabase &schedDB, cta::log::LogContext& lc, const cta::xrd::Request* request);
};

FailedRequestLsWriteReactor::FailedRequestLsWriteReactor(cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, const std::string& instanceName, SchedulerDatabase &schedDB, cta::log::LogContext& lc, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::FAILEDREQUEST_LS) {    
    initializeStream<cta::cmdline::FailedRequestLsResponseStream>(request, catalogue, scheduler, instanceName, schedDB, lc);
}

} // namespace cta::frontend::grpc