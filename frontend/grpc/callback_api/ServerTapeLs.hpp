#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/TapeLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class TapeLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    TapeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                      cta::Scheduler& scheduler,
                      const std::string& instanceName,
                      const uint64_t missingFileCopiesMinAgeSecs,
                      const cta::xrd::Request* request);
};

TapeLsWriteReactor::TapeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                      cta::Scheduler& scheduler,
                                      const std::string& instanceName,
                                      const uint64_t missingFileCopiesMinAgeSecs,
                                      const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::TAPE_LS) {
    initializeStream<cta::cmdline::TapeLsResponseStream>(request, catalogue, scheduler, instanceName, missingFileCopiesMinAgeSecs);
}

}  // namespace cta::frontend::grpc