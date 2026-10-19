/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <catalogue/Catalogue.hpp>
#include <grpcpp/grpcpp.h>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

class DefaultWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
public:
  explicit DefaultWriteReactor(const std::string& errMsg,
                               ::grpc::StatusCode errCode = ::grpc::StatusCode::INVALID_ARGUMENT) {
    Finish(::grpc::Status(errCode, errMsg));
  }

  void OnDone() override;
};

inline void DefaultWriteReactor::OnDone() {
  delete this;
}

}  // namespace cta::admin_api::rpc
