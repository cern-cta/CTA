/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "cmdline/CtaAdminParsedCmd.hpp"
#include "version.h"
#include <grpcpp/grpcpp.h>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

namespace cta::admin {

class CtaAdminGrpcCmd {
public:
  //! Send the protocol buffer across the gRPC transport
  void send(const CtaAdminParsedCmd& parsedCmd, std::string endpoint) const;
};

}  // namespace cta::admin
