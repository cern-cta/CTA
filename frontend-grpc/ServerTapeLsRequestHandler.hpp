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
#include "catalogue/TapeSearchCriteria.hpp"
#include "cta_grpc_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace cta {
namespace frontend {
namespace grpc {
namespace server {

class AsyncServer;

class TapeLsRequestHandler : public request::IHandler {
public:
  TapeLsRequestHandler() = delete;
  TapeLsRequestHandler(cta::log::Logger& log,
                       AsyncServer& asyncServer,
                       cta::frontend::rpc::CtaRpcStream::AsyncService& ctaRpcStreamSvc);
  ~TapeLsRequestHandler() override;

  void init() override {};             //  Nothnig todo
  bool next(const bool bOk) override;  // can thorw

private:
  const unsigned int CHUNK_SIZE = 4 * 1024;

  enum class StreamState : unsigned int { NEW = 1, PROCESSING, WRITE, ERROR, FINISH };

  cta::log::Logger& m_log;
  cta::frontend::grpc::request::Tag m_tag;
  AsyncServer& m_asyncServer;
  cta::frontend::rpc::CtaRpcStream::AsyncService& m_ctaRpcStreamSvc;
  StreamState m_streamState;
  /* 
   * Context for the rpc, allowing to tweak aspects of it such as the use
   * of compression, authentication, as well as to send metadata back to the
   * client.
   */
  ::grpc::ServerContext m_ctx;

  // Request from the client
  cta::frontend::rpc::AdminRequest m_request;
  // Response send back to the client
  cta::frontend::rpc::StreamResponse m_response;
  // // The means to get back to the client.
  ::grpc::ServerAsyncWriter<cta::frontend::rpc::StreamResponse> m_writer;
  cta::catalogue::TapeSearchCriteria m_searchCriteria;
  std::list<common::dataStructures::Tape> m_tapeList;
};

}  // namespace server
}  // namespace grpc
}  // namespace frontend
}  // namespace cta