#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "cmdline/CtaAdminResponseStream.hpp"

#pragma once

namespace cta::frontend::grpc {

/* This is the base class from which all commands will inherit,
 * we introduce it to avoid having to write boilerplate code (OnDone, OnWriteDone) for each command */
class CtaAdminServerWriteReactor
    : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
public:
  CtaAdminServerWriteReactor(cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler,
                             const std::string& instanceName,
                             std::unique_ptr<cta::cmdline::CtaAdminResponseStream> stream,
                             cta::admin::HeaderType headerType);
  void OnWriteDone(bool ok) override;
  void OnDone() override;
  virtual void NextWrite();  // this needs to be virtual for the Version command only
protected:                   // so that the child classes can access those as if they were theirs
  bool m_isHeaderSent;       // or could be a static variable in the function NextWrite()
  cta::xrd::StreamResponse m_response;
  std::optional<std::string> m_schedulerBackendName;
  std::string m_instanceName;
  std::unique_ptr<cta::cmdline::CtaAdminResponseStream> m_stream;
  cta::admin::HeaderType m_headerType;
};
}  // namespace cta::frontend::grpc