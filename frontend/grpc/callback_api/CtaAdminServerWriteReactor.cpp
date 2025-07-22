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
    if (!ok) {
        Finish(::grpc::Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
    }
    NextWrite();
}

void CtaAdminServerWriteReactor::OnDone() {
    delete this;
}

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName)
  : m_isHeaderSent(false),
    m_schedulerBackendName(scheduler.getSchedulerBackendName()),
    m_instanceName(instanceName) {}
}