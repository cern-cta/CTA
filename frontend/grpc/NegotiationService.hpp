/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IHandler.hpp"
#include "ServerNegotiationRequestHandler.hpp"
#include "TokenStorage.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <thread>
#include <vector>

#include "cta_frontend.grpc.pb.h"

namespace cta::frontend::grpc::server {

/**
 * Lightweight service for handling Kerberos authentication negotiation.
 * This service runs asynchronously on its own completion queue but shares
 * the main server's ServerBuilder and listening port.
 */
class NegotiationService {
public:
  NegotiationService(cta::log::LogContext& lc,
                     TokenStorage& tokenStorage,
                     std::unique_ptr<::grpc::ServerCompletionQueue> cq,
                     const std::string& keytab,
                     const std::string& servicePrincipal,
                     unsigned int numThreads = 1);

  ~NegotiationService();

  // Non-copyable, non-movable
  NegotiationService(const NegotiationService&) = delete;
  NegotiationService& operator=(const NegotiationService&) = delete;
  NegotiationService(NegotiationService&&) = delete;
  NegotiationService& operator=(NegotiationService&&) = delete;

  NegotiationRequestHandler& registerHandler() {
    m_lc.log(cta::log::INFO, "In NegotiationService::registerHandler");
    std::lock_guard<std::mutex> lck(m_mtxLockHandler);
    std::unique_ptr<NegotiationRequestHandler> upHandler =
      std::make_unique<NegotiationRequestHandler>(m_lc.logger(), *this, m_service, m_keytab, m_servicePrincipal);
    // Handler initialisation
    upHandler->init();  // can throw
    // Store address
    uintptr_t tag = reinterpret_cast<std::uintptr_t>(upHandler.get());
    // Move ownership & store under the Tag
    m_umapHandlers[upHandler.get()] = std::move(upHandler);
    return *m_umapHandlers[reinterpret_cast<cta::frontend::grpc::request::Tag>(tag)];
  }

  ::grpc::ServerCompletionQueue& completionQueue() { return *m_upCompletionQueue; }

  /**
   * Get token storage (for handler access)
   */
  TokenStorage& tokenStorage() { return m_tokenStorage; }

  /**
   * Get the underlying gRPC async service for registration with ServerBuilder
   */
  cta::xrd::Negotiation::AsyncService& getService() { return m_service; }

  NegotiationRequestHandler& getHandler(const cta::frontend::grpc::request::Tag tag);
  void releaseHandler(const cta::frontend::grpc::request::Tag tag);
  void startProcessing();

private:
  void process(unsigned int threadId);

  cta::log::LogContext m_lc;
  TokenStorage& m_tokenStorage;
  std::unique_ptr<::grpc::ServerCompletionQueue> m_upCompletionQueue = nullptr;
  std::string m_keytab;
  std::string m_servicePrincipal;
  const unsigned int m_numThreads = 1;

  cta::xrd::Negotiation::AsyncService m_service;
  std::vector<std::thread> m_threads;
  std::unordered_map<cta::frontend::grpc::request::Tag, std::unique_ptr<NegotiationRequestHandler>> m_umapHandlers;
  std::mutex m_mtxLockHandler;
};

}  // namespace cta::frontend::grpc::server
