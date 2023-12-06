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
 
#include "ClientNegotiationRequestHandler.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"

cta::frontend::grpc::client::NegotiationRequestHandler::NegotiationRequestHandler(cta::log::Logger& log,
                                                                cta::frontend::rpc::Negotiation::Stub& stub,
                                                                ::grpc::CompletionQueue& completionQueue,
                                                                const std::string& strSpn
                                                              ) :
                                                                m_log(log),
                                                                m_stub(stub),
                                                                m_completionQueue(completionQueue),
                                                                m_strSpn(strSpn),
                                                                m_tag(this),
                                                                m_streamState(StreamState::NEW)
                                                                {
  // Get KRB5 principal name for the given service                                                                
  m_gssNameSpn = gssSpn(m_strSpn);

}

void cta::frontend::grpc::client::NegotiationRequestHandler::logGSSErrors(const std::string& strContext, OM_uint32 gssCode, int iType) {
  
  log::LogContext lc(m_log);
  log::ScopedParamContainer params(lc);
  std::ostringstream osMsgScopeParam;
  OM_uint32 gssMinStat;
  gss_buffer_desc gssMsg;
  OM_uint32 gssMsgCtx = 0;
  
  params.add("tag", m_tag);
  /*
   * Because gss_display_status() only displays one status code at a time,
   * and some functions can return multiple status conditions,
   * it should be invoked as part of a loop.
   */
  do {
    gss_display_status(&gssMinStat, gssCode, iType, GSS_C_NULL_OID,
                             &gssMsgCtx, &gssMsg);
    osMsgScopeParam  << "GSS-API-ERROR:" << gssMsgCtx;
    params.add(osMsgScopeParam.str(), std::string((char *)gssMsg.value));
    osMsgScopeParam.str(""); // reset ostringstream
    gss_release_buffer(&gssMinStat, &gssMsg);
  } while(gssMsgCtx);
  
  lc.log(cta::log::ERR, strContext);
}

gss_name_t cta::frontend::grpc::client::NegotiationRequestHandler::gssSpn(const std::string& strSpn) {
  log::LogContext lc(m_log);
  OM_uint32 gssMajStat;
  OM_uint32 gssMinStat;
  gss_buffer_desc gssNameBuf = GSS_C_EMPTY_BUFFER;
  gss_name_t gssNameSpn = GSS_C_NO_NAME;

  gssNameBuf.length = strSpn.size();
  gssNameBuf.value = const_cast<char*>(strSpn.c_str());;

  gssMajStat = gss_import_name(&gssMinStat, &gssNameBuf, GSS_KRB5_NT_PRINCIPAL_NAME, &gssNameSpn);

  if (GSS_ERROR(gssMajStat)) {
    logGSSErrors("In grpc::server::NegotiationRequestHandlerClient::gssSpn(): gss_import_name() major status.", gssMajStat, GSS_C_GSS_CODE);
    logGSSErrors("In grpc::server::NegotiationRequestHandlerClient::gssSpn(): gss_import_name() minor status.", gssMinStat, GSS_C_MECH_CODE);
    throw cta::exception::Exception("In grpc::server::NegotiationRequestHandlerClient::gssSpn(): Failed to import gss name.");
  }

  return gssNameSpn;
}

bool cta::frontend::grpc::client::NegotiationRequestHandler::next(const bool bOk) {
  bool bNext = false;
  log::LogContext lc(m_log);
  std::string strErrorMsg;
  
  const uint8_t* pChallengeData = nullptr;
  
  // Check the state and report an error
  if(!bOk) {
    switch (m_streamState) {
      case StreamState::WRITE:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::client::NegotiationRequestHandler::next(): Server has been shut down before receiving a matching request.");
        }
        break;
      case StreamState::READ:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::INFO, "In grpc::client::NegotiationRequestHandler::next(): End of stream.");
        }
        break;
      case StreamState::FINISH:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::INFO, "In grpc::client::NegotiationRequestHandler::next(): Request processing finished.");        
        }
        break;
      default:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::client::NegotiationRequestHandler::next(): Request processing aborted, call is cancelled or connection is dropped.");
        }
        break;
    }
    m_streamState = StreamState::FINISH;
    m_uprwNegotiation->Finish(&m_grpcStatus, m_tag);
    // return bNext;
    return true;
  }
  // else everything is OK
  bNext = true;
  
  switch (m_streamState) {
    case StreamState::NEW:
      // CMD: prepare Negotiate stream
      m_uprwNegotiation = m_stub.PrepareAsyncNegotiate(&m_ctx, &m_completionQueue);
      m_streamState = StreamState::WRITE;
      // Initiates the gRPC call
      m_uprwNegotiation->StartCall(m_tag);
      break;
    case StreamState::READ:
      m_streamState = StreamState::WRITE;
      m_response.Clear();
      m_uprwNegotiation->Read(&m_response, m_tag);// is not blocking
      break;
    case StreamState::WRITE:
      if(!m_response.is_complete()) {
        // If not first call
        if(m_response.challenge().empty() && m_gssRecvToken.value != GSS_C_NO_BUFFER) {
            log::ScopedParamContainer params(lc);
            params.add("tag", m_tag);
            lc.log(cta::log::ERR, "In grpc::client::NegotiationRequestHandler::next(): Request processing failed, challenge expected");
            m_streamState = StreamState::FINISH;
            m_uprwNegotiation->WritesDone(m_tag);
        } else {
          pChallengeData = reinterpret_cast<const uint8_t*>(m_response.challenge().c_str()); 
          m_gssRecvToken.length = m_response.challenge().size();
          m_gssRecvToken.value = const_cast<void*>(reinterpret_cast<const void*>(pChallengeData));
          
          m_gssMajStat = gss_init_sec_context( //
                        &m_gssMinStat, // minor_status
                        m_gssCred, // claimant_cred_handle
                        &m_gssCtx, // context_handle
                        m_gssNameSpn, // target_name
                        GSS_C_NO_OID, // mech_type of the desired mechanism
                        m_gssRetFlags, // req_flags
                        0, // time_req for the context to remain valid. 0 for default lifetime.
                        GSS_C_NO_CHANNEL_BINDINGS, // channel bindings
                        // GSS_C_NO_BUFFER, // input token
                        &m_gssRecvToken, // input token
                        nullptr, // actual_mech_type
                        &m_gssSendToken, // output token 
                        nullptr, // ret_flags
                        nullptr // time_req
                    );
                    
          switch (m_gssMajStat) {
            // https://www.ietf.org/archive/id/draft-perez-krb-wg-gss-preauth-03.html
            case GSS_S_CONTINUE_NEEDED:
                m_streamState = StreamState::READ;
                m_request.set_challenge(std::string(reinterpret_cast<const char*>(m_gssSendToken.value), m_gssSendToken.length));
                m_request.set_service_principal_name(m_strSpn);
                gss_release_buffer(&m_gssMinStat, &m_gssSendToken);
                m_uprwNegotiation->Write(m_request, m_tag);
                
              break;
            case GSS_S_COMPLETE:
              m_streamState = StreamState::FINISH;
              m_uprwNegotiation->WritesDone(m_tag);
              break;
            case GSS_S_DEFECTIVE_TOKEN:
            case GSS_S_DEFECTIVE_CREDENTIAL:
            case GSS_S_NO_CRED:
            case GSS_S_CREDENTIALS_EXPIRED:
            case GSS_S_BAD_BINDINGS:
            case GSS_S_NO_CONTEXT:
            case GSS_S_BAD_SIG:
            case GSS_S_OLD_TOKEN:
            case GSS_S_DUPLICATE_TOKEN:
            case GSS_S_BAD_MECH:
            case GSS_S_FAILURE:
              m_streamState = StreamState::FINISH;
              logGSSErrors("In grpc::client::NegotiationRequestHandler::next(): gss_accept_sec_context() major status.", m_gssMajStat, GSS_C_GSS_CODE);
              logGSSErrors("In grpc::client::NegotiationRequestHandler::nect(): gss_accept_sec_context() minor status.", m_gssMinStat, GSS_C_MECH_CODE);
              m_uprwNegotiation->WritesDone(m_tag);
              break;
            default:
              // No default
              break;
          }
        }
      } else {
        m_streamState = StreamState::FINISH;
        m_strToken = m_response.token();
        m_uprwNegotiation->WritesDone(m_tag);
      }
      break;
    case StreamState::FINISH:
      bNext = false;
      if (m_gssCtx != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&m_gssMinStat, &m_gssCtx, GSS_C_NO_BUFFER);
        if (m_gssMinStat != GSS_S_COMPLETE) {
          logGSSErrors("In grpc::client::NegotiationRequestHandler::next(): gss_delete_sec_context() minor status.", m_gssMinStat, GSS_C_GSS_CODE);
        }
      }
      gss_release_cred(&m_gssMinStat, &m_gssCred);
      gss_release_buffer(&m_gssMinStat, &m_gssSendToken);
      gss_release_name(&m_gssMinStat, &m_gssNameSpn);
      
      switch(m_grpcStatus.error_code()) {
        case ::grpc::OK:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              lc.log(cta::log::INFO, "In grpc::client::NegotiationRequestHandler::next(): Request processing finished.");
            }
            break;
        case ::grpc::CANCELLED:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              lc.log(cta::log::INFO, "In grpc::client::NegotiationRequestHandler::next(): Request processing canceled.");
            }
            break;
        default:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              params.add("message", m_grpcStatus.error_message());
              lc.log(cta::log::INFO, "In grpc::client::NegotiationRequestHandler::next(): gRPC Error.");

            }
            break;
      }
    default:
      // no default
      break;
  }
  
  return bNext;
}

