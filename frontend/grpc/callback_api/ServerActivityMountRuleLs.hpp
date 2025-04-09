#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/ActivityMountRuleLsResponseStream.hpp"

namespace cta::frontend::grpc {

class ActivityMountRuleLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

ActivityMountRuleLsWriteReactor::ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::ACTIVITYMOUNTRULE_LS) {
    initializeStream<cta::cmdline::ActivityMountRuleLsResponseStream>(request, catalogue, scheduler, instanceName);
}

} // namespace cta::frontend::grpc