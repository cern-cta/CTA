#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/RequesterMountRuleLsResponseStream.hpp"

namespace cta::frontend::grpc {

class RequesterMountRuleLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                     cta::Scheduler& scheduler,
                                     const std::string& instanceName,
                                     const cta::xrd::Request* request);

};

RequesterMountRuleLsWriteReactor::RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                                   cta::Scheduler& scheduler,
                                                                   const std::string& instanceName,
                                                                   const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::REQUESTERMOUNTRULE_LS)
{
    initializeStream<cta::cmdline::RequesterMountRuleLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc