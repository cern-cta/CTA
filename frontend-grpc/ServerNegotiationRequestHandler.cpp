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
 
#include "ServerNegotiationRequestHandler.hpp"
#include "AsyncServer.hpp"
#include "RequestMessage.hpp"
#include "common/log/LogContext.hpp"

#include <cryptopp/base64.h>

cta::frontend::grpc::server::NegotiationRequestHandler::NegotiationRequestHandler(cta::log::Logger& log,
                                                                AsyncServer& asyncServer,
                                                                cta::frontend::rpc::Negotiation::AsyncService& ctaNegotiationSvc,
                                                                const std::string& strKeytab,
                                                                const std::string& strService
                                                              ) :
                                                                m_log(log),
                                                                m_tag(this),
                                                                m_asyncServer(asyncServer),
                                                                m_ctaNegotiationSvc(ctaNegotiationSvc),
                                                                m_strKeytab(strKeytab),
                                                                m_strService(strService),
                                                                m_streamState(StreamState::NEW),
                                                                m_gssCtx(GSS_C_NO_CONTEXT),
                                                                m_rwNegotiation(&m_ctx)
                                                                {
                                                                  
  
}

cta::frontend::grpc::server::NegotiationRequestHandler::~NegotiationRequestHandler() {
  OM_uint32 gssMajStat, gssMinStat;
  
  gssMajStat = gss_release_cred(&gssMinStat, &m_serverCreds);
  if (gssMajStat != GSS_S_COMPLETE) {
    logGSSErrors("In grpc::server::NegotiationRequestHandler::~NegotiationRequestHandler(): gss_release_cred() major status.", gssMajStat, GSS_C_GSS_CODE);
    logGSSErrors("IN grpc::server::NegotiationRequestHandler::~NegotiationRequestHandler(): gss_release_cred() minor status", gssMinStat, GSS_C_MECH_CODE);
  }
  
}

void cta::frontend::grpc::server::NegotiationRequestHandler::init() {
    gss_OID mech = GSS_C_NO_OID;
    registerKeytab(m_strKeytab);
    acquireCreds(m_strService, mech, &m_serverCreds);
}

void cta::frontend::grpc::server::NegotiationRequestHandler::logGSSErrors(const std::string& strContext, OM_uint32 gssCode, int iType) {
  
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

void cta::frontend::grpc::server::NegotiationRequestHandler::registerKeytab(const std::string& strKeytab) {
  OM_uint32 gssMajStat;
  
  gssMajStat = krb5_gss_register_acceptor_identity(m_strKeytab.c_str());
  if (gssMajStat != GSS_S_COMPLETE) {
    logGSSErrors("In grpc::server::NegotiationRequestHandler::registerKeytab(): krb5_gss_register_acceptor_identity() major status.", gssMajStat, GSS_C_GSS_CODE);
    throw cta::exception::Exception("In grpc::server::NegotiationRequestHandler::registerKeytab(): Failed to register keytab: " + strKeytab);
  }
}

void cta::frontend::grpc::server::NegotiationRequestHandler::releaseName(const std::string& strContext, gss_name_t* pGssName) {
  OM_uint32 gssMajStat, gssMinStat;
  
  gssMajStat = gss_release_name(&gssMinStat, pGssName);
  if (gssMajStat != GSS_S_COMPLETE) {
    logGSSErrors(strContext + " gss_release_name() major status.", gssMajStat, GSS_C_GSS_CODE);
    logGSSErrors(strContext + " gss_release_name() minor status.", gssMinStat, GSS_C_MECH_CODE);
  }
}

void cta::frontend::grpc::server::NegotiationRequestHandler::acquireCreds(const std::string& strService, gss_OID gssMech, gss_cred_id_t *pGssServerCreds) {
  gss_buffer_desc gssNameBuf;
  gss_name_t gssServerName;
  OM_uint32 gssMajStat, gssMinStat;
  gss_OID_set_desc gssMechlist;
  gss_OID_set gssMechs = GSS_C_NO_OID_SET;

  gssNameBuf.value = const_cast<char*>(strService.c_str());
  gssNameBuf.length = strService.size() + 1;
  gssMajStat = gss_import_name(&gssMinStat, &gssNameBuf,
                             (gss_OID) gss_nt_service_name, &gssServerName);
  if (gssMajStat != GSS_S_COMPLETE) {
    logGSSErrors("In grpc::server::NegotiationRequestHandler::acquireCreds(): gss_import_name() major status.", gssMajStat, GSS_C_GSS_CODE);
    logGSSErrors("In grpc::server::NegotiationRequestHandler::acquireCreds(): gss_import_name() minor status.", gssMinStat, GSS_C_MECH_CODE);
    throw cta::exception::Exception("In grpc::server::NegotiationRequestHandler::acquireCreds(): Failed to get Kerberos credentials");
  }

  if (gssMech != GSS_C_NO_OID) {
    gssMechlist.count = 1;
    gssMechlist.elements = gssMech;
    gssMechs = &gssMechlist;
  }
  gssMajStat = gss_acquire_cred(&gssMinStat, gssServerName, 0, gssMechs, GSS_C_ACCEPT,
                              pGssServerCreds, NULL, NULL);

  releaseName("In grpc::server::NegotiationRequestHandler::acquireCreds():", &gssServerName);
  
  if (gssMajStat != GSS_S_COMPLETE) {
    logGSSErrors("In grpc::server::NegotiationRequestHandler::acquireCreds(): gss_acquire_cred() major status.", gssMajStat, GSS_C_GSS_CODE);
    logGSSErrors("In grpc::server::NegotiationRequestHandler::acquireCreds(): gss_acquire_cred() minor status.", gssMinStat, GSS_C_MECH_CODE);
    throw cta::exception::Exception("In grpc::server::NegotiationRequestHandler::acquireCreds(): Failed to get Kerberos credentials.");
  }

  
}

bool cta::frontend::grpc::server::NegotiationRequestHandler::next(const bool bOk) {
  bool bNext = false;
  log::LogContext lc(m_log);
  
  // Check the state and report an error
  if(!bOk) {
    switch (m_streamState) {
      case StreamState::PROCESSING:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::server::NegotiationRequestHandler::next(): Server has been shut down before receiving a matching request.");
        }
        break;
      default:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::server::NegotiationRequestHandler::next(): Request processing aborted: call is cancelled or connection is dropped.");
        }
        break;
    }
    return bNext;
  }
  // else everything is OK
  bNext = true;
  
  switch (m_streamState) {
    case StreamState::NEW:
      m_ctaNegotiationSvc.RequestNegotiate(&m_ctx, &m_rwNegotiation, &m_asyncServer.completionQueue(), &m_asyncServer.completionQueue(), m_tag);
      m_streamState = StreamState::PROCESSING;
      break;
    case StreamState::PROCESSING:
      m_asyncServer.registerHandler<cta::frontend::grpc::server::NegotiationRequestHandler>(m_ctaNegotiationSvc, m_strKeytab, m_strService).next(bOk);
      m_rwNegotiation.Read(&m_request, m_tag);// read first m_request
      m_streamState = StreamState::WRITE;
      break;
    case StreamState::READ:
      m_rwNegotiation.Read(&m_request, m_tag);
      m_streamState = StreamState::WRITE;
      break;
    case StreamState::WRITE:
      {
        // Accept sec context
        gss_buffer_desc gssRecvToken {0, GSS_C_NO_BUFFER};// length, value
        gss_buffer_desc gssSendToken {0, GSS_C_NO_BUFFER};;
        gss_name_t gssSrcName;
        gss_OID gssOidMechType;
        OM_uint32 gssMajStat;
        OM_uint32 gssAccSecMinStat;
        OM_uint32 gssMinStat;
        OM_uint32 gssRetFlags;
        
        const uint8_t* pChallengeData = reinterpret_cast<const uint8_t*>(m_request.challenge().c_str()); 
        gssRecvToken.length = m_request.challenge().size();
        gssRecvToken.value = const_cast<void*>(reinterpret_cast<const void*>(pChallengeData));
        
        gssMajStat = gss_accept_sec_context(&gssAccSecMinStat, &m_gssCtx,
                                          m_serverCreds, &gssRecvToken,
                                          GSS_C_NO_CHANNEL_BINDINGS,
                                          &gssSrcName, &gssOidMechType, &gssSendToken,
                                          &gssRetFlags,
                                          NULL,  /* time_rec */
                                          NULL); /* del_cred_handle */
        
        switch (gssMajStat) {
          // https://www.ietf.org/archive/id/draft-perez-krb-wg-gss-preauth-03.html
          case GSS_S_CONTINUE_NEEDED:
              m_response.set_is_complete(false);
              m_response.set_challenge(std::string(reinterpret_cast<const char*>(gssSendToken.value), gssSendToken.length));
              m_response.set_token("");
              gss_release_buffer(&gssMinStat, &gssSendToken);
              m_rwNegotiation.Write(m_response, m_tag);
              m_streamState = StreamState::READ;
            break;
          case GSS_S_COMPLETE:
            m_streamState = StreamState::FINISH;
            m_response.set_is_complete(true);
            m_response.set_challenge("");
            /*
             * The token can be of any type
             * now KRB token is used
             */
            m_response.set_token(std::string(reinterpret_cast<const char*>(gssRecvToken.value), gssRecvToken.length));
            m_asyncServer.tokenStorage().store(std::string(reinterpret_cast<const char*>(gssRecvToken.value), gssRecvToken.length), m_request.service_principal_name());
            m_rwNegotiation.Write(m_response, m_tag);
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
            m_streamState = StreamState::ERROR;
            logGSSErrors("In grpc::server::NegotiationRequestHandler::next(): gss_accept_sec_context() major status.", gssMajStat, gssAccSecMinStat);
            m_response.set_is_complete(false);
            m_response.set_challenge("");
            m_response.set_token("");
            m_response.set_error_msg("Negotiation request failed");
            gss_release_buffer(&gssMinStat, &gssSendToken);
            m_rwNegotiation.Write(m_response, m_tag);
            break;
          default:
            // No default
            break;
        }
      
        releaseName("In grpc::server::NegotiationRequestHandler::next():", &gssSrcName);
      }
      
      break;
    case StreamState::ERROR:
      m_streamState = StreamState::FINISH;
      m_rwNegotiation.Finish(::grpc::Status::CANCELLED, m_tag);
      break;
    case StreamState::FINISH:
      if (m_gssCtx != GSS_C_NO_CONTEXT) {
        OM_uint32 gssMinStat;
        gss_delete_sec_context(&gssMinStat, &m_gssCtx, GSS_C_NO_BUFFER);
        if (gssMinStat != GSS_S_COMPLETE) {
          logGSSErrors("In grpc::server::NegotiationRequestHandler::next(): gss_delete_sec_context() minor status", gssMinStat, GSS_C_GSS_CODE);
        }
      }
      // gss_release_buffer(&gssMinStat, &gssSendToken);
      bNext = false;
      break;
    default:
      // no default
      break;
  }
  
  return bNext;
}
  
