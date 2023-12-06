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
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>

namespace cta::frontend::grpc::client {

class NegotiationRequestHandler : public request::IHandler {

public:
  NegotiationRequestHandler() = delete;
  NegotiationRequestHandler(cta::log::Logger& log, cta::frontend::rpc::Negotiation::Stub& stub, ::grpc::CompletionQueue& completionQueue, const std::string& strSpn);
  ~NegotiationRequestHandler() override = default;
  
  void init() override {}; //  Nothnig todo
  bool next(const bool bOk) override; // can thorw
  
  inline const std::string token() {
    return m_strToken;
  }

private:
  enum class StreamState : unsigned int {
    NEW = 1,
    WRITE,
    READ,
    FINISH
  };
  
  cta::log::Logger& m_log;
  cta::frontend::rpc::Negotiation::Stub& m_stub;
  ::grpc::CompletionQueue&  m_completionQueue;
  const std::string& m_strSpn;
  cta::frontend::grpc::request::Tag m_tag;
  
  StreamState m_streamState;
  
  
  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ::grpc::ClientContext m_ctx;
  ::grpc::Status        m_grpcStatus;
  // Request from the client
  cta::frontend::rpc::KerberosAuthenticationRequest m_request;
  // Response send back to the client
  cta::frontend::rpc::KerberosAuthenticationResponse m_response;
  // ClientAsyncReaderWriter
  std::unique_ptr<::grpc::ClientAsyncReaderWriter<cta::frontend::rpc::KerberosAuthenticationRequest, cta::frontend::rpc::KerberosAuthenticationResponse>> m_uprwNegotiation;
  // Token from the negotiation service
  std::string m_strToken = {""};


  // GSS
  gss_buffer_desc m_gssRecvToken {0, GSS_C_NO_BUFFER};// length, value
  gss_buffer_desc m_gssSendToken {0, GSS_C_NO_BUFFER};
  gss_name_t m_gssNameSpn;
  gss_ctx_id_t m_gssCtx = {GSS_C_NO_CONTEXT};
  OM_uint32 m_gssMajStat = {GSS_S_CONTINUE_NEEDED};
  OM_uint32 m_gssMinStat;
  OM_uint32 m_gssRetFlags = {GSS_C_REPLAY_FLAG | GSS_C_SEQUENCE_FLAG | GSS_C_MUTUAL_FLAG};
  gss_cred_id_t m_gssCred = {GSS_C_NO_CREDENTIAL};

  void logGSSErrors(const std::string& strContext, OM_uint32 gssCode, int iType);
  gss_name_t gssSpn(const std::string& strSpn);
    
};

} // namespace cta::frontend::grpc::client
