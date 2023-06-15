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
#include "cta_grpc_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>

namespace cta {
namespace frontend {
namespace grpc {
namespace server {

class AsyncServer;

class NegotiationRequestHandler : public request::IHandler {
public:
  NegotiationRequestHandler() = delete;
  NegotiationRequestHandler(cta::log::Logger& log,
                            AsyncServer& asyncServer,
                            cta::frontend::rpc::Negotiation::AsyncService& ctaRpcStreamSvc,
                            const std::string& strKeytab,
                            const std::string& strService);
  ~NegotiationRequestHandler() override;

  void init() override;                // can thorw
  bool next(const bool bOk) override;  // can thorw

private:
  const unsigned int CHUNK_SIZE = 4 * 1024;

  enum class StreamState : unsigned int { NEW = 1, PROCESSING, READ, WRITE, ERROR, FINISH };

  cta::log::Logger& m_log;
  cta::frontend::grpc::request::Tag m_tag;
  AsyncServer& m_asyncServer;
  cta::frontend::rpc::Negotiation::AsyncService& m_ctaNegotiationSvc;
  std::string m_strKeytab;
  std::string m_strService;
  StreamState m_streamState;
  gss_ctx_id_t m_gssCtx;
  gss_cred_id_t m_serverCreds = {GSS_C_NO_CREDENTIAL};
  /* 
   * Context for the rpc, allowing to tweak aspects of it such as the use
   * of compression, authentication, as well as to send metadata back to the
   * client.
   */
  ::grpc::ServerContext m_ctx;

  // Request from the client
  cta::frontend::rpc::KerberosAuthenticationRequest m_request;
  // Response send back to the client
  cta::frontend::rpc::KerberosAuthenticationResponse m_response;
  // The means to get back to the client.
  ::grpc::ServerAsyncReaderWriter<cta::frontend::rpc::KerberosAuthenticationResponse,
                                  cta::frontend::rpc::KerberosAuthenticationRequest>
    m_rwNegotiation;
  // KRB5 credentials
  void logGSSErrors(const std::string& strContext, OM_uint32 gssCode, int iType);
  void registerKeytab(const std::string& strKeytab);  // can throw
  void releaseName(const std::string& strContext, gss_name_t* pGssName);
  void acquireCreds(const std::string& strService, gss_OID mech, gss_cred_id_t* pGssServerCreds);  // can throw
};

}  // namespace server
}  // namespace grpc
}  // namespace frontend
}  // namespace cta