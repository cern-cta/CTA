/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "cta_frontend.pb.h"
#include "frontend/common/AdminCmdOptions.hpp"

#include <map>
#include <optional>
#include <string>

namespace cta::frontend::grpc::request {

class RequestMessage : public cta::frontend::AdminCmdOptions {
public:
  explicit RequestMessage(const cta::xrd::Request& request);
  ~RequestMessage() final = default;

  /*!
   * Get a required option
   */
  using AdminCmdOptions::getOptional;
  using AdminCmdOptions::getRequired;
  using AdminCmdOptions::has_flag;
  // TODO:
  // const Versions &getClientVersions() const {
  //   return m_client_versions;
  // }
  // const std::string &getClientXrdSsiProtoIntVersion() const {
  //   return m_client_xrd_ssi_proto_int_version;
  // }

private:
};

}  // namespace cta::frontend::grpc::request
