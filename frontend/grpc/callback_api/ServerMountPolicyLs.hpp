#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/MountPolicyLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class MountPolicyLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    MountPolicyLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler,
                             const std::string& instanceName,
                             const cta::xrd::Request* request);

};

MountPolicyLsWriteReactor::MountPolicyLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                    cta::Scheduler& scheduler,
                                                    const std::string& instanceName,
                                                    const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::MOUNTPOLICY_LS) {
    initializeStream<cta::cmdline::MountPolicyLsResponseStream>(request, catalogue, scheduler, instanceName);
}


}  // namespace cta::frontend::grpc