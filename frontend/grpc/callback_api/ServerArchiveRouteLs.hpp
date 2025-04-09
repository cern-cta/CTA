#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/ArchiveRouteLsResponseStream.hpp"

namespace cta::frontend::grpc {

class ArchiveRouteLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
};

ArchiveRouteLsWriteReactor::ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::ARCHIVEROUTE_LS) {
    initializeStream<cta::cmdline::ArchiveRouteLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc