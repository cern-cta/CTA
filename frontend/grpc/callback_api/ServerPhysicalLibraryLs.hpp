#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/PhysicalLibraryLsResponseStream.hpp"

namespace cta::frontend::grpc {

class PhysicalLibraryLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                 cta::Scheduler& scheduler,
                                 const std::string& instanceName,
                                 const cta::xrd::Request* request);

};

PhysicalLibraryLsWriteReactor::PhysicalLibraryLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                            cta::Scheduler& scheduler,
                                                            const std::string& instanceName,
                                                            const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::PHYSICALLIBRARY_LS) {
    initializeStream<cta::cmdline::PhysicalLibraryLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc