// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

/* This is the base class from which all commands will inherit,
 * we introduce it to avoid having to write boilerplate code (OnDone, OnWriteDone) for each command */
class CtaAdminServerWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        CtaAdminServerWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, std::string instanceName, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In CtaAdminServerWriteReactor, we are inside OnWriteDone" << std::endl;
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
            NextWrite();
        }
        void OnDone() override;
        virtual void NextWrite() = 0;
    protected: // so that the child classes can access those as if they were theirs
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::optional<std::string> m_schedulerBackendName;
        std::string m_instanceName;
};

void CtaAdminServerWriteReactor::OnDone() {
    std::cout << "In CtaAdminServerWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, std::string instanceName, const cta::xrd::Request* request)
  : m_isHeaderSent(false),
    m_schedulerBackendName(scheduler.getSchedulerBackendName()),
    m_instanceName(instanceName) {}
}