#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/MediaTypeLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class MediaTypeLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    MediaTypeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           const cta::xrd::Request* request);

};

MediaTypeLsWriteReactor::MediaTypeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                cta::Scheduler& scheduler,
                                                const std::string& instanceName,
                                                const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::MEDIATYPE_LS) {
    initializeStream<cta::cmdline::MediaTypeLsResponseStream>(request, catalogue, scheduler, instanceName);
}


} // namespace cta::frontend::grpc