// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/ShowQueuesResponseStream.hpp"

namespace cta::frontend::grpc {

class ShowQueuesWriteReactor : public CtaAdminServerWriteReactor {
public:
    ShowQueuesWriteReactor(cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::string& instanceName,
                       cta::log::LogContext lc,
                       const cta::xrd::Request* request);
};

ShowQueuesWriteReactor::ShowQueuesWriteReactor(cta::catalogue::Catalogue& catalogue,
                                       cta::Scheduler& scheduler,
                                       const std::string& instanceName,
                                       cta::log::LogContext lc,
                                       const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName, cta::admin::HeaderType::SHOWQUEUES) {
    
    try {
        m_stream = std::make_unique<cta::cmdline::ShowQueuesResponseStream>(
            catalogue, scheduler, instanceName, lc);
        
        m_stream->init(request->admincmd());
        
        NextWrite();
    } catch (const std::exception& ex) {
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.what()));
    }
}

} // namespace cta::frontend::grpc