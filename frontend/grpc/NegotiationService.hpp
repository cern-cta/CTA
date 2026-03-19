/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "IHandler.hpp"
#include "ServerNegotiationRequestHandler.hpp"
#include "TokenStorage.hpp"
#include "common/exception/Exception.hpp"
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
  /**
   * Thread-safe wrapper around the handler map.
   * Encapsulates synchronization so callers don't need to manage mutexes.
   */
  class ThreadSafeHandlerMap {
  public:
    using Tag = cta::frontend::grpc::request::Tag;

    NegotiationRequestHandler& insert(std::unique_ptr<NegotiationRequestHandler> handler) {
      Tag tag = handler.get();
      std::lock_guard<std::mutex> lck(m_mtxLockHandler);
      m_umapHandlers[tag] = std::move(handler);
      return *m_umapHandlers[tag];
    }

    NegotiationRequestHandler& get(Tag tag) {
      std::lock_guard<std::mutex> lck(m_mtxLockHandler);
      /*
      * Check if the handler is registered;
      */
      const auto itorFind = m_umapHandlers.find(tag);
      if (itorFind == m_umapHandlers.end()) {
        std::ostringstream osExMsg;
        osExMsg << "In ThreadSafeHandlerMap::get(): Handler " << tag << " is not registered.";
        throw cta::exception::Exception(osExMsg.str());
      }
      const std::unique_ptr<NegotiationRequestHandler>& upHandler = itorFind->second;
      return *upHandler;
    }

    void release(cta::log::Logger& log, Tag tag) {
      {
        cta::log::LogContext lc(log);
        log::ScopedParamContainer params(lc);
        params.add("handler", tag);
        lc.log(cta::log::DEBUG, "In ThreadSafeHandlerMap::release(): Release handler.");
      }
      std::lock_guard<std::mutex> lck(m_mtxLockHandler);
      /*
       * Check if the handler is registered;
       */
      const auto itorFind = m_umapHandlers.find(tag);
      if (itorFind == m_umapHandlers.end()) {
        std::ostringstream osExMsg;
        osExMsg << "In ThreadSafeHandlerMap::release(): Handler " << tag << " is not registered.";
        throw cta::exception::Exception(osExMsg.str());
      }
      m_umapHandlers.erase(tag);
    }

    auto begin() { return m_umapHandlers.begin(); }

    auto end() { return m_umapHandlers.end(); }

  private:
    std::unordered_map<Tag, std::unique_ptr<NegotiationRequestHandler>> m_umapHandlers;
    std::mutex m_mtxLockHandler;
  };

  NegotiationService(cta::log::Logger& log,
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

  NegotiationRequestHandler& registerHandler();

  ::grpc::ServerCompletionQueue& completionQueue() { return *m_upCompletionQueue; }

  /**
   * Get token storage (for handler access)
   */
  TokenStorage& tokenStorage() { return m_tokenStorage; }

  /**
   * Get the underlying gRPC async service for registration with ServerBuilder
   */
  cta::xrd::Negotiation::AsyncService& getService() { return m_service; }

  void startProcessing();

private:
  // Static to force explicit dependency passing for thread safety.
  // Each thread creates its own LogContext from the shared Logger.
  static void process(unsigned int threadId,
                      ::grpc::ServerCompletionQueue& cq,
                      cta::log::Logger& log,
                      ThreadSafeHandlerMap& handlers);

  static NegotiationRequestHandler& getHandler(const cta::frontend::grpc::request::Tag tag,
                                               ThreadSafeHandlerMap& umapHandlers);

  static void releaseHandler(const cta::frontend::grpc::request::Tag tag,
                             cta::log::Logger& log,
                             ThreadSafeHandlerMap& umapHandlers);

  cta::log::Logger& m_log;  // Logger is thread-safe; each thread creates its own LogContext from it
  TokenStorage& m_tokenStorage;
  std::unique_ptr<::grpc::ServerCompletionQueue> m_upCompletionQueue = nullptr;
  std::string m_keytab;
  std::string m_servicePrincipal;
  const unsigned int m_numThreads = 1;

  cta::xrd::Negotiation::AsyncService m_service;
  std::vector<std::thread> m_threads;
  ThreadSafeHandlerMap m_handlers;
};

}  // namespace cta::frontend::grpc::server
