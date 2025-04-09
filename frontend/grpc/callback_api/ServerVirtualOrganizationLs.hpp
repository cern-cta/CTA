#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/VirtualOrganizationLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class VirtualOrganizationLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                     cta::Scheduler& scheduler,
                                     const std::string& instanceName,
                                     const cta::xrd::Request* request);
};

VirtualOrganizationLsWriteReactor::VirtualOrganizationLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                                    cta::Scheduler& scheduler,
                                                                    const std::string& instanceName,
                                                                    const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::VIRTUALORGANIZATION_LS) {
    initializeStream<cta::cmdline::VirtualOrganizationLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc