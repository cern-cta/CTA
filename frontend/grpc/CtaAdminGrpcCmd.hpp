/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "cmdline/CtaAdminParsedCmd.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "version.h"

#include <grpcpp/grpcpp.h>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin {

class CtaAdminGrpcCmd {
public:
  //! Send the protocol buffer across the gRPC transport
  void send(const CtaAdminParsedCmd& parsedCmd, const std::string& config_file);

private:
  void setupKrb5AuthenticatedAdminCall(std::shared_ptr<grpc::Channel> spChannelNegotiation,
                                       grpc::ClientContext& context,
                                       const std::string& GSS_SPN,
                                       cta::log::FileLogger& log);
  // Attaches the Kerberos token to the call metadata (per-call credentials)

  void setupJwtAuthenticatedAdminCall(grpc::ClientContext& context, const std::string& token_path);
  // Attaches the JWT token to the call metadata (per-call credentials)
};

}  // namespace cta::admin
