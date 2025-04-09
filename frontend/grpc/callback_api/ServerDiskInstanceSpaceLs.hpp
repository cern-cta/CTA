#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/DiskInstanceSpaceLsResponseStream.hpp"

namespace cta::frontend::grpc {

class DiskInstanceSpaceLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                   cta::Scheduler& scheduler,
                                   const std::string& instanceName,
                                   const cta::xrd::Request* request);

};

DiskInstanceSpaceLsWriteReactor::DiskInstanceSpaceLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                                cta::Scheduler& scheduler,
                                                                const std::string& instanceName,
                                                                const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::DISKINSTANCESPACE_LS) {
    initializeStream<cta::cmdline::DiskInstanceSpaceLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc