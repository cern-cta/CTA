/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "NegotiationService.hpp"

#include "ServerNegotiationRequestHandler.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace cta::frontend::grpc::server {

NegotiationService::NegotiationService(cta::log::Logger& log,
                                       TokenStorage& tokenStorage,
                                       std::unique_ptr<::grpc::ServerCompletionQueue> cq,
                                       const std::string& keytab,
                                       const std::string& servicePrincipal,
                                       unsigned int numThreads)
    : m_log(log),
      m_tokenStorage(tokenStorage),
      m_upCompletionQueue(std::move(cq)),
      m_keytab(keytab),
      m_servicePrincipal(servicePrincipal),
      m_numThreads(numThreads) {}

NegotiationService::~NegotiationService() {
  // Always shutdown the completion queue after the server.
  if (m_upCompletionQueue) {
    m_upCompletionQueue->Shutdown();
    // Flush the completion queue
    void* pTag;
    bool bOk = false;
    while (m_upCompletionQueue->Next(&pTag, &bOk)) {
      // Discard remaining events
    }
  }

  // Join all processing threads
  for (auto& thread : m_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

NegotiationRequestHandler& NegotiationService::registerHandler() {
    cta::log::LogContext lc(m_log);
    lc.log(cta::log::INFO, "In NegotiationService::registerHandler");
    std::lock_guard<std::mutex> lck(m_mtxLockHandler);
    std::unique_ptr<NegotiationRequestHandler> upHandler =
      std::make_unique<NegotiationRequestHandler>(m_log, *this, m_service, m_keytab, m_servicePrincipal);
    // Handler initialisation
    upHandler->init();  // can throw
    // Store address
    uintptr_t tag = reinterpret_cast<std::uintptr_t>(upHandler.get());
    // Move ownership & store under the Tag
    m_umapHandlers[upHandler.get()] = std::move(upHandler);
    return *m_umapHandlers[reinterpret_cast<cta::frontend::grpc::request::Tag>(tag)];
  }

NegotiationRequestHandler& NegotiationService::getHandler(const cta::frontend::grpc::request::Tag tag) {
  std::lock_guard<std::mutex> lck(m_mtxLockHandler);
  /*
   * Check if the handler is registered;
   */
  const auto itorFind = m_umapHandlers.find(tag);
  if (itorFind == m_umapHandlers.end()) {
    std::ostringstream osExMsg;
    osExMsg << "In NegotiationService::getHandler(): Handler " << tag << " is not registered.";
    throw cta::exception::Exception(osExMsg.str());
  }
  const std::unique_ptr<NegotiationRequestHandler>& upHandler = itorFind->second;
  return *upHandler;
}

void NegotiationService::releaseHandler(const cta::frontend::grpc::request::Tag tag) {
  std::lock_guard<std::mutex> lck(m_mtxLockHandler);

  {
    cta::log::LogContext lc(m_log);
    log::ScopedParamContainer params(lc);
    params.add("handler", tag);
    lc.log(cta::log::DEBUG, "In NegotiationService::releaseHandler(): Release handler.");
  }
  /*
   * Check if the handler is registered;
   */
  const auto itorFind = m_umapHandlers.find(tag);
  if (itorFind == m_umapHandlers.end()) {
    std::ostringstream osExMsg;
    osExMsg << "In NegotiationService::releaseHandler(): Handler " << tag << " is not registered.";
    throw cta::exception::Exception(osExMsg.str());
  }
  m_umapHandlers.erase(itorFind);
}

void NegotiationService::startProcessing() {
  cta::log::LogContext lc(m_log);

  // Register handlers first
  {
    log::ScopedParamContainer params(lc);
    params.add("keytab", m_keytab);
    params.add("servicePrincipal", m_servicePrincipal);
    lc.log(cta::log::INFO, "Initializing Kerberos negotiation handlers");

    // Create initial handler instances
    // Each handler will spawn a new one when it starts processing
    for (unsigned int i = 0; i < m_numThreads; ++i) {
      try {
        registerHandler();
      } catch (const cta::exception::Exception& e) {
        log::ScopedParamContainer errorParams(lc);
        errorParams.add("error", e.getMessageValue());
        lc.log(cta::log::ERR, "Failed to initialize negotiation handler");
        throw;
      }
    }
  }

  // Initialise all registered handlers
  for (const auto& item : m_umapHandlers) {
    const std::unique_ptr<NegotiationRequestHandler>& upIHandler = item.second;
    upIHandler.get()->next(true);
    //TODO: Log names of initialised handlers;
  }

  // Start worker threads
  m_threads.reserve(m_numThreads);
  for (unsigned int i = 0; i < m_numThreads; ++i) {
    m_threads.emplace_back(&NegotiationService::process, this, i, std::ref(m_log));
  }

  log::ScopedParamContainer params(lc);
  params.add("threads", m_numThreads);
  lc.log(cta::log::INFO, "Negotiation service processing threads started");
}

void NegotiationService::process(unsigned int threadId, cta::log::Logger& log) {
  // Each thread creates its own LogContext - thread-safe
  cta::log::LogContext lc(log);

  lc.log(cta::log::INFO, "In NegotiationService::process");
  /*
   *  pTag
   *  Uniquely identifies a request.
   *  Uses the address of handler object as the unique tag for the call
   */
  void* pTag = nullptr;
  bool bOk = false;
  while (true) {
    /*
     * Block waiting to read the next event from the completion queue.
     * The return value of Next should always be checked.
     * It tells whether there is any kind of event or m_upCompletionQueue is shutting down.
     */
    if (!m_upCompletionQueue->Next(&pTag, &bOk)) {
      log::ScopedParamContainer params(lc);
      params.add("thread", threadId);
      lc.log(cta::log::ERR, "In NegotiationService::process(): The completion queue has been shutdown.");
      break;
    }
    if (!pTag) {
      log::ScopedParamContainer params(lc);
      params.add("thread", threadId);
      lc.log(cta::log::ERR, "In NegotiationService::process(): Invalid tag delivered by notification queue.");
      break;
    }

    try {
      // Everything is ok the request can be processed
      NegotiationRequestHandler& handler = getHandler(static_cast<cta::frontend::grpc::request::Tag>(pTag));
      if (!handler.next(bOk)) {
        releaseHandler(static_cast<cta::frontend::grpc::request::Tag>(pTag));
      }
    } catch (const cta::exception::Exception& ex) {
      log::ScopedParamContainer params(lc);
      params.add("thread", threadId);
      params.add("exceptionMessage", ex.getMessageValue());
      lc.log(log::ERR, "NegotiationService::process(): Got an exception.");
    }
  }
}

}  // namespace cta::frontend::grpc::server
