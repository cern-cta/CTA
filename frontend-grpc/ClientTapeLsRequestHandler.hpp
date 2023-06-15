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

#include "IHandler.hpp"
#include "common/log/Logger.hpp"
#include "cmdline/CtaAdminTextFormatter.hpp"
#include "cta_grpc_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace cta {
namespace frontend {
namespace grpc {
namespace client {

class TapeLsRequestHandler : public request::IHandler {
public:
  TapeLsRequestHandler() = delete;
  TapeLsRequestHandler(cta::log::Logger& log,
                       cta::frontend::rpc::CtaRpcStream::Stub& stub,
                       ::grpc::CompletionQueue& completionQueue,
                       cta::admin::TextFormatter& textFormatter,
                       cta::frontend::rpc::AdminRequest& request);
  ~TapeLsRequestHandler() override;

  void init() override {};             //  Nothnig todo
  bool next(const bool bOk) override;  // can thorw

private:
  enum class StreamState : unsigned int { NEW = 1, REQUEST, HEADER, READ, FINISH };

  cta::log::Logger& m_log;
  cta::frontend::rpc::CtaRpcStream::Stub& m_stub;
  ::grpc::CompletionQueue& m_completionQueue;
  cta::admin::TextFormatter& m_textFormatter;
  // Request from the client
  cta::frontend::rpc::AdminRequest& m_request;
  cta::frontend::grpc::request::Tag m_tag;

  StreamState m_streamState;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ::grpc::ClientContext m_ctx;
  ::grpc::Status m_grpcStatus;
  // Response send back to the client
  cta::frontend::rpc::StreamResponse m_response;
  std::unique_ptr<::grpc::ClientAsyncReader<cta::frontend::rpc::StreamResponse>> m_upReader;
};

}  // namespace client
}  // namespace grpc
}  // namespace frontend
}  // namespace cta