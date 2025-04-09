#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

/* This is the base class from which all commands will inherit,
 * we introduce it to avoid having to write boilerplate code (OnDone, OnWriteDone) for each command */
void CtaAdminServerWriteReactor::OnWriteDone(bool ok) {
    std::cout << "In CtaAdminServerWriteReactor, we are inside OnWriteDone" << std::endl;
    if (!ok) {
        std::cout << "Unexpected failure in OnWriteDone" << std::endl;
        Finish(::grpc::Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
    }
    std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
    NextWrite();
}

void CtaAdminServerWriteReactor::OnDone() {
    std::cout << "In CtaAdminServerWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName)
  : m_isHeaderSent(false),
    m_schedulerBackendName(scheduler.getSchedulerBackendName()),
    m_instanceName(instanceName) {}
}