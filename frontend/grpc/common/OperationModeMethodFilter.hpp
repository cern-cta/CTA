/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "frontend/common/OperationModes.hpp"

#include <grpcpp/security/auth_metadata_processor.h>
#include <set>
#include <string>

namespace cta::frontend::grpc::common {

const std::set<std::string, std::less<>> WFE_METHODS = {
  "/cta.xrd.CtaRpc/Create",
  "/cta.xrd.CtaRpc/Archive",
  "/cta.xrd.CtaRpc/Retrieve",
  "/cta.xrd.CtaRpc/Delete",
  "/cta.xrd.CtaRpc/CancelRetrieve",
};

const std::set<std::string, std::less<>> ADMIN_METHODS = {
  "/cta.xrd.CtaRpc/Admin",
  "/cta.xrd.CtaRpcStream/GenericAdminStream",
};

/**
 * @brief Rejects RPC methods which do not belong to the frontend's operation mode
 *
 * Health check, reflection, etc... remain usable on both paths.
 *
 */
struct OperationModeMethodFilter final : ::grpc::AuthMetadataProcessor {
  explicit OperationModeMethodFilter(OperationMode mode)
      : m_modeName(toString(mode)),
        m_deniedMethods(mode == OperationMode::WFE ? ADMIN_METHODS : WFE_METHODS) {}

  bool IsBlocking() const override { return false; }

  ::grpc::Status
  Process(const InputMetadata& authMetadata, ::grpc::AuthContext*, OutputMetadata*, OutputMetadata*) override {
    const auto iter = authMetadata.find(":path");
    if (iter == authMetadata.end()) {
      // Should never happen, but without the path we cannot tell which method was requested
      return {::grpc::StatusCode::INTERNAL, "Unable to determine the requested gRPC method"};
    }

    if (const std::string_view path(iter->second.data(), iter->second.size()); m_deniedMethods.contains(path)) {
      return {::grpc::StatusCode::UNIMPLEMENTED,
              "Method " + std::string(path) + " is not available in operation mode '" + m_modeName + "'"};
    }
    return ::grpc::Status::OK;
  }

private:
  const std::string m_modeName;
  const std::set<std::string, std::less<>>& m_deniedMethods;
};

}  // namespace cta::frontend::grpc::common
