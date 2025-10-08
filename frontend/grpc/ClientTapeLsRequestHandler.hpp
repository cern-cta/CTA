/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IHandler.hpp"
#include "common/log/Logger.hpp"
#include "cmdline/CtaAdminTextFormatter.hpp"
#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace cta::frontend::grpc::client {

class TapeLsRequestHandler : public request::IHandler {

public:
  TapeLsRequestHandler() = delete;
  TapeLsRequestHandler(cta::log::Logger& log,
                       cta::xrd::CtaRpcStream::Stub& stub,
                       ::grpc::CompletionQueue& completionQueue,
                       cta::admin::TextFormatter& textFormatter,
                       cta::xrd::Request& request);
  ~TapeLsRequestHandler() override = default;

  void init() override {}; //  Nothnig todo
  bool next(const bool bOk) override; // can thorw

private:
  enum class StreamState : unsigned int {
    NEW = 1,
    REQUEST,
    HEADER,
    READ,
    FINISH
  };

  cta::log::Logger& m_log;
  cta::xrd::CtaRpcStream::Stub& m_stub;
  ::grpc::CompletionQueue&  m_completionQueue;
  cta::admin::TextFormatter& m_textFormatter;
  // Request from the client
  cta::xrd::Request& m_request;
  cta::frontend::grpc::request::Tag m_tag;

  StreamState m_streamState;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ::grpc::ClientContext m_ctx;
  ::grpc::Status        m_grpcStatus;
  // Response send back to the client
  cta::xrd::StreamResponse m_response;
  std::unique_ptr<::grpc::ClientAsyncReader<cta::xrd::StreamResponse>> m_upReader;
};

} // namespace cta::frontend::grpc::client
