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
#include "cta_frontend.grpc.pb.h"
#include "IHandler.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"

#include <grpcpp/grpcpp.h>

#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <thread>

namespace cta::frontend::grpc::client {

template <class STUB>
class AsyncClient {

public:
  
  AsyncClient() = delete;
  AsyncClient(cta::log::Logger& log, const std::shared_ptr<::grpc::Channel>& spChannel) :
                                                                    m_log(log),
                                                                    m_upStub(STUB::NewStub(spChannel))
                                                                  {
                                                                  }
                                                                  
  ~AsyncClient() {
    m_completionQueue.Shutdown();
  }
  
  // Delete default construcotrs
  AsyncClient(const AsyncClient&)            = delete;
  AsyncClient& operator=(const AsyncClient&) = delete;
  AsyncClient(AsyncClient&&)                 = delete;
  AsyncClient& operator=(AsyncClient&&)      = delete;

  /**
   * Exe
   */
  template<class HANDLER, class... ARGS>   std::unique_ptr<HANDLER> exe(ARGS&... args) {
    std::unique_ptr<cta::frontend::grpc::request::IHandler> upHandler = std::make_unique<HANDLER>(m_log, *m_upStub, m_completionQueue, args...);
    // Handler initialisation
    upHandler->init();// can throw
    // Initilization
    upHandler->next(true);
    
    void* pTag = nullptr;
    bool bOk = false;
    
    log::LogContext lc(m_log);
       
    // Process
    while(true) {
      if(!m_completionQueue.Next(&pTag, &bOk)) {
        // Release the handler before throwing
        log::ScopedParamContainer params(lc);
        params.add("tag", pTag);
        lc.log(cta::log::DEBUG, "In rpc::AsyncClient::exe(): Release handler.");
        upHandler.reset();
        throw cta::exception::Exception("In grpc::AsyncClient::exe(): The completion queue has been shutdown.");
      }
      if(!pTag) {
        // Release the handler before throwing
        log::ScopedParamContainer params(lc);
        params.add("tag", pTag);
        lc.log(cta::log::DEBUG, "In rpc::AsyncClient::exe(): Release handler.");
        upHandler.reset();
        throw cta::exception::Exception("In grpc::AsyncClient::exe(): Invalid tag delivered by notification queue.");
      }
      
      if(!upHandler->next(bOk)) {
        break;
      }
      
    } // End while(true)
  
    {
      log::ScopedParamContainer params(lc);
      params.add("tag", pTag);
      lc.log(cta::log::DEBUG, "In rpc::AsyncClient::exe(): Release handler.");
    }
    // unique_ptr cannot be cast so, release->cast->create;
    std::unique_ptr<HANDLER> upReturnHandler(reinterpret_cast<HANDLER*>(upHandler.release()));
    
    return upReturnHandler; // applies std::move
    
  }

private:
  cta::log::Logger& m_log;
  std::unique_ptr<typename STUB::Stub> m_upStub;
  ::grpc::CompletionQueue              m_completionQueue;
};

} // namespace cta::frontend::grpc::client
