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
#include "catalogue/Catalogue.hpp"
#include "common/log/Logger.hpp"
#include "common/exception/Exception.hpp"

#include <grpcpp/grpcpp.h>

#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <thread>

namespace cta::frontend::grpc::server {

class CallbackAsyncServer {

public:
  
  CallbackAsyncServer() = delete;
  CallbackAsyncServer(cta::log::Logger& log, cta::catalogue::Catalogue& catalogue, const unsigned int uiPort) ;
  ~CallbackAsyncServer();
  // Delete default construcotrs
  CallbackAsyncServer(const CallbackAsyncServer&)            = delete;
  CallbackAsyncServer& operator=(const CallbackAsyncServer&) = delete;
  CallbackAsyncServer(CallbackAsyncServer&&)                 = delete;
  CallbackAsyncServer& operator=(CallbackAsyncServer&&)      = delete;
  /**
   * Register service
   */
  template<class SERVICE, class... ARGS> void registerService(ARGS... args) {
    std::lock_guard<std::mutex> lck(m_mtxLockService);
   
    std::unique_ptr<::grpc::Service> upService; // Empty
    upService = std::make_unique<SERVICE>(std::move(args)...);
    /*
     * Register a service.
     * This call does not take ownership of the service.
     * The service must exist for the lifetime of the Server instance returned by BuildAndStart().
     */
    m_upServerBuilder->RegisterService(upService.get());
    // Move ownership
  }


  cta::catalogue::Catalogue& catalogue() {
    return m_catalogue;
  }

  void run(const std::shared_ptr<::grpc::ServerCredentials>& spServerCredentials);

private:
  cta::log::Logger& m_log;
  cta::catalogue::Catalogue& m_catalogue;
  std::unique_ptr<::grpc::Server>                m_upServer;
  unsigned int                                   m_uiPort;
  const unsigned int                             m_uiNoThreads = 1; 
  std::mutex m_mtxLockHandler;
  std::mutex m_mtxLockService;
  std::unique_ptr<::grpc::ServerBuilder> m_upServerBuilder;
  //
  void process();
};

} // namespace cta::frontend::grpc::server
