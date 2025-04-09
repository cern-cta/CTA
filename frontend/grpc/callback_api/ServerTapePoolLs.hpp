#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/TapePoolLsResponseStream.hpp"

namespace cta::frontend::grpc {

class TapePoolLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    TapePoolLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                          cta::Scheduler& scheduler,
                          const std::string& instanceName,
                          const cta::xrd::Request* request);
};

TapePoolLsWriteReactor::TapePoolLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                              cta::Scheduler& scheduler,
                                              const std::string& instanceName,
                                              const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::TAPEPOOL_LS) {
    initializeStream<cta::cmdline::TapePoolLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc