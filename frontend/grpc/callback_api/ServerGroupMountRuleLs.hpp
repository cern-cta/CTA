#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/GroupMountRuleLsResponseStream.hpp"

namespace cta::frontend::grpc {

class GroupMountRuleLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

GroupMountRuleLsWriteReactor::GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::GROUPMOUNTRULE_LS) {
    initializeStream<cta::cmdline::GroupMountRuleLsResponseStream>(request, catalogue, scheduler, instanceName);
    
}

} // namespace cta::frontend::grpc