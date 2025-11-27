/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

} // namespace cta::frontend::grpc::request
