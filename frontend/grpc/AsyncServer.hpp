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

#include "cta_grpc_frontend.grpc.pb.h"
#include "IHandler.hpp"
#include "ServiceAuthProcessor.hpp"
#include "TokenStorage.hpp"
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

class AsyncServer {

public:
  
  AsyncServer() = delete;
  AsyncServer(cta::log::Logger& log, cta::catalogue::Catalogue& catalogue, TokenStorage& tokenStorage, const unsigned int uiPort, const unsigned int uiNoThreads = 1) ;
  ~AsyncServer();
  // Delete default construcotrs
  AsyncServer(const AsyncServer&)            = delete;
  AsyncServer& operator=(const AsyncServer&) = delete;
  AsyncServer(AsyncServer&&)                 = delete;
  AsyncServer& operator=(AsyncServer&&)      = delete;
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
    m_vServices.push_back(std::move(upService));
  }
  /**
   * Get servcie
   */
  template<class SERVICE> SERVICE& service() {
    SERVICE* pService = nullptr;
    std::unique_lock<std::mutex> lck(m_mtxLockService);
    for(std::unique_ptr<::grpc::Service>& upService : m_vServices) {
      pService = dynamic_cast<SERVICE*>(upService.get());
      if(pService != nullptr) {
        return *pService;
      }
    }
    throw cta::exception::Exception("In grpc::AsyncServer::service(): Service is not registered.");
  }
  /**
   * Register handler
   * can throw
   */
  template<class HANDLER, class... ARGS> cta::frontend::grpc::request::IHandler& registerHandler(ARGS&... args) {
    std::lock_guard<std::mutex> lck(m_mtxLockHandler);
    std::unique_ptr<cta::frontend::grpc::request::IHandler> upHandler = std::make_unique<HANDLER>(m_log, *this, args...);
    // Handler initialisation
    upHandler->init();// can throw
    // Store address
    uintptr_t tag = reinterpret_cast<std::uintptr_t>(upHandler.get());
    // Move ownership & store under the Tag
    m_umapHandlers[upHandler.get()] = std::move(upHandler);
    // Retrun IHandler
    return *m_umapHandlers[reinterpret_cast<cta::frontend::grpc::request::Tag>(tag)];
  }
  /**
   * Register handler
   * can throw
   */
  template<class HANDLER, class SERVICE, class... ARGS> void registerHandler(ARGS&... args) {
    std::lock_guard<std::mutex> lck(m_mtxLockHandler);
    std::unique_ptr<cta::frontend::grpc::request::IHandler> upHandler = std::make_unique<HANDLER>(m_log, *this, service<SERVICE>(), args...);
    // Handler initialisation
    upHandler->init();// can throw
    // Move ownership & store under the Tag
    m_umapHandlers[upHandler.get()] = std::move(upHandler);
  }

  ::grpc::ServerCompletionQueue& completionQueue() {
    return *m_upCompletionQueue;
  }

  cta::catalogue::Catalogue& catalogue() {
    return m_catalogue;
  }

  TokenStorage& tokenStorage() {
    return m_tokenStorage;
  }
  
  cta::frontend::grpc::request::IHandler& getHandler(const cta::frontend::grpc::request::Tag tag);
  void releaseHandler(const cta::frontend::grpc::request::Tag tag);
  void run(const std::shared_ptr<::grpc::ServerCredentials>& spServerCredentials, const std::shared_ptr<::grpc::AuthMetadataProcessor>& spAuthProcessor);

private:
  cta::log::Logger& m_log;
  cta::catalogue::Catalogue& m_catalogue;
  TokenStorage& m_tokenStorage;
  std::unique_ptr<::grpc::ServerCompletionQueue> m_upCompletionQueue;
  std::unique_ptr<::grpc::Server>                m_upServer;
  unsigned int                                   m_uiPort;
  const unsigned int                             m_uiNoThreads = 1; 
  std::unordered_map<cta::frontend::grpc::request::Tag, std::unique_ptr<cta::frontend::grpc::request::IHandler>> m_umapHandlers;
  std::vector<std::unique_ptr<::grpc::Service>>  m_vServices;
  std::mutex m_mtxLockHandler;
  std::mutex m_mtxLockService;
  std::unique_ptr<::grpc::ServerBuilder> m_upServerBuilder;
  std::vector<std::thread> m_vThreads;
  std::shared_ptr<::grpc::AuthMetadataProcessor> m_spAuthProcessor = nullptr;
  //
  void process(unsigned int uiId);
};

} // namespace cta::frontend::grpc::server
