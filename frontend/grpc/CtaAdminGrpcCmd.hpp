/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "cmdline/CtaAdminParsedCmd.hpp"
#include "version.h"
#include <grpcpp/grpcpp.h>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"

namespace cta::admin {

class CtaAdminGrpcCmd {
public:
  //! Send the protocol buffer across the gRPC transport
  void send(const CtaAdminParsedCmd& parsedCmd, const std::string& config_file);

private:
  std::shared_ptr<::grpc::Channel>
  setupKrb5AuthenticatedAdminCall(std::shared_ptr<grpc::Channel> spChannelNegotiation,
                                  grpc::ClientContext& context,
                                  const std::string& GRPC_SERVER,
                                  const std::string& GSS_SPN,
                                  std::shared_ptr<grpc::ChannelCredentials> credentials,
                                  cta::log::FileLogger& log);
  // credentials are already setup in the main function - send
  // all this function does is attach the token to the call metadata
  void setupJwtAuthenticatedAdminCall(grpc::ClientContext& context, const std::string& token_path);
};

}  // namespace cta::admin
