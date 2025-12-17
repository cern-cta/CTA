/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <catalogue/Catalogue.hpp>
#include <grpcpp/grpcpp.h>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::frontend::grpc {

class DefaultWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
public:
  explicit DefaultWriteReactor(const std::string& errMsg,
                               ::grpc::StatusCode errCode = ::grpc::StatusCode::INVALID_ARGUMENT) {
    Finish(Status(errCode, errMsg));
  }

  void OnDone() override;
};

void DefaultWriteReactor::OnDone() {
  delete this;
}

}  // namespace cta::frontend::grpc
