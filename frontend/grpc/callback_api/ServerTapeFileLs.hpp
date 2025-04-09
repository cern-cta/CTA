#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/TapeFileLsResponseStream.hpp"

namespace cta::frontend::grpc {

class TapeFileLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    TapeFileLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                          cta::Scheduler& scheduler,
                          const std::string& instanceName,
                          const cta::xrd::Request* request);
};

TapeFileLsWriteReactor::TapeFileLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                              cta::Scheduler& scheduler,
                                              const std::string& instanceName,
                                              const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::TAPEFILE_LS) {
    initializeStream<cta::cmdline::TapeFileLsResponseStream>(request, catalogue, scheduler, instanceName);
}
} // namespace cta::frontend::grpc