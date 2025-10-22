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

#include "NegotiationService.hpp"
#include "ServerNegotiationRequestHandler.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"

namespace cta::frontend::grpc::server {

NegotiationService::NegotiationService(cta::log::LogContext& lc,
                                       TokenStorage& tokenStorage,
                                       std::unique_ptr<::grpc::ServerCompletionQueue> cq,
                                       const std::string& keytab,
                                       const std::string& servicePrincipal,
                                       unsigned int numThreads)
    : m_lc(lc),
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
    log::ScopedParamContainer params(m_lc);
    params.add("handler", tag);
    m_lc.log(cta::log::DEBUG, "In NegotiationService::releaseHandler(): Release handler.");
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
  // Register handlers first
  {
    log::ScopedParamContainer params(m_lc);
    params.add("keytab", m_keytab);
    params.add("servicePrincipal", m_servicePrincipal);
    m_lc.log(cta::log::INFO, "Initializing Kerberos negotiation handlers");

    // Create initial handler instances
    // Each handler will spawn a new one when it starts processing
    for (unsigned int i = 0; i < m_numThreads; ++i) {
      try {
        registerHandler();
      } catch (const cta::exception::Exception& e) {
        log::ScopedParamContainer errorParams(m_lc);
        errorParams.add("error", e.getMessageValue());
        m_lc.log(cta::log::ERR, "Failed to initialize negotiation handler");
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
    m_threads.emplace_back(&NegotiationService::process, this, i);
  }

  log::ScopedParamContainer params(m_lc);
  params.add("threads", m_numThreads);
  m_lc.log(cta::log::INFO, "Negotiation service processing threads started");
}

void NegotiationService::process(unsigned int threadId) {
  log::LogContext lc(m_lc);
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
