#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/LogicalLibraryLsResponseStream.hpp"

namespace cta::frontend::grpc {

class LogicalLibraryLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                cta::Scheduler& scheduler,
                                const std::string& instanceName,
                                const cta::xrd::Request* request);

};

LogicalLibraryLsWriteReactor::LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                          cta::Scheduler& scheduler,
                                                          const std::string& instanceName,
                                                          const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::LOGICALLIBRARY_LS) {
    initializeStream<cta::cmdline::LogicalLibraryLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc