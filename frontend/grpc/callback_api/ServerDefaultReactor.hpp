#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace cta::frontend::grpc {

class DefaultWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
public:
  DefaultWriteReactor(const std::string& errMsg, ::grpc::StatusCode errCode = ::grpc::StatusCode::INVALID_ARGUMENT) {
    Finish(Status(errCode, errMsg));
  }

  void OnDone() override;
};

void DefaultWriteReactor::OnDone() {
  delete this;
}

}  // namespace cta::frontend::grpc