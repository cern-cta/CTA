/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IHandler.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/log/Logger.hpp"

#include <grpcpp/grpcpp.h>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::frontend::grpc::server {

class AsyncServer;

class TapeLsRequestHandler : public request::IHandler {
public:
  TapeLsRequestHandler() = delete;
  TapeLsRequestHandler(cta::log::Logger& log,
                       AsyncServer& asyncServer,
                       cta::xrd::CtaRpcStream::AsyncService& ctaRpcStreamSvc);
  ~TapeLsRequestHandler() override = default;

  void init() override {};             //  Nothnig todo
  bool next(const bool bOk) override;  // can thorw

private:
  const unsigned int CHUNK_SIZE = 4 * 1024;

  enum class StreamState : unsigned int { NEW = 1, PROCESSING, WRITE, ERROR, FINISH };

  cta::log::Logger& m_log;
  cta::frontend::grpc::request::Tag m_tag;
  AsyncServer& m_asyncServer;
  cta::xrd::CtaRpcStream::AsyncService& m_ctaRpcStreamSvc;
  StreamState m_streamState;
  /*
   * Context for the rpc, allowing to tweak aspects of it such as the use
   * of compression, authentication, as well as to send metadata back to the
   * client.
   */
  ::grpc::ServerContext m_ctx;

  // Request from the client
  cta::xrd::Request m_request;
  // Response send back to the client
  cta::xrd::StreamResponse m_response;
  // // The means to get back to the client.
  ::grpc::ServerAsyncWriter<cta::xrd::StreamResponse> m_writer;
  cta::catalogue::TapeSearchCriteria m_searchCriteria;
  std::list<common::dataStructures::Tape> m_tapeList;
};

}  // namespace cta::frontend::grpc::server
