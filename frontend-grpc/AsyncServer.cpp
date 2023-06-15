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

#include "AsyncServer.hpp"

#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"

#include <grpcpp/security/server_credentials.h>

#include <stdexcept>  // std::out_of_range

cta::frontend::grpc::server::AsyncServer::AsyncServer(cta::log::Logger& log,
                                                      cta::catalogue::Catalogue& catalogue,
                                                      TokenStorage& tokenStorage,
                                                      unsigned int uiPort,
                                                      const unsigned int uiNoThreads) :
m_log(log),
m_catalogue(catalogue),
m_tokenStorage(tokenStorage),
m_uiPort(uiPort),
m_uiNoThreads(uiNoThreads),
m_upServerBuilder(std::make_unique<::grpc::ServerBuilder>()) {}

cta::frontend::grpc::server::AsyncServer::~AsyncServer() {
  if (m_upServer) {
    m_upServer->Shutdown();
  }
  // Always shutdown the completion queue after the server.
  if (m_upCompletionQueue) {
    m_upCompletionQueue->Shutdown();
    // Flush the completion queue
    void* pTag;
    bool bOk = false;
    while (m_upCompletionQueue->Next(&pTag, &bOk)) {}
  }
}

cta::frontend::grpc::request::IHandler&
  cta::frontend::grpc::server::AsyncServer::getHandler(const cta::frontend::grpc::request::Tag tag) {
  std::lock_guard<std::mutex> lck(m_mtxLockHandler);
  /*
   * Check if the handler is registered;
   */
  std::unordered_map<cta::frontend::grpc::request::Tag,
                     std::unique_ptr<cta::frontend::grpc::request::IHandler>>::const_iterator itorFind =
    m_umapHandlers.find(tag);
  if (itorFind == m_umapHandlers.end()) {
    std::ostringstream osExMsg;
    osExMsg << "In grpc::AsyncServer::getHandler(): Handler " << tag << " is not registered.";
    throw cta::exception::Exception(osExMsg.str());
  }
  const std::unique_ptr<cta::frontend::grpc::request::IHandler>& upHandler = itorFind->second;
  return *upHandler;
}

void cta::frontend::grpc::server::AsyncServer::releaseHandler(const cta::frontend::grpc::request::Tag tag) {
  std::lock_guard<std::mutex> lck(m_mtxLockHandler);

  {
    log::LogContext lc(m_log);
    log::ScopedParamContainer params(lc);
    params.add("handler", tag);
    lc.log(cta::log::DEBUG, "In grpc::AsyncServer::releaseHandler(): Release handler.");
  }
  /*
   * Check if the handler is registered;
   */
  std::unordered_map<cta::frontend::grpc::request::Tag, std::unique_ptr<request::IHandler>>::const_iterator itorFind =
    m_umapHandlers.find(tag);
  if (itorFind == m_umapHandlers.end()) {
    std::ostringstream osExMsg;
    osExMsg << "In grpc::AsyncServer::releaseHandler(): Handler " << tag << " is not registered.";
    throw cta::exception::Exception(osExMsg.str());
  }
  m_umapHandlers.erase(itorFind);
}

void cta::frontend::grpc::server::AsyncServer::run(
  const std::shared_ptr<::grpc::ServerCredentials>& spServerCredentials,
  const std::shared_ptr<::grpc::AuthMetadataProcessor>& spAuthProcessor) {
  log::LogContext lc(m_log);
  m_upCompletionQueue = m_upServerBuilder->AddCompletionQueue();
  //
  if (!spServerCredentials) {
    throw cta::exception::Exception("In grpc::AsyncServer::run(): Incorrect server credentials.");
  }
  if (!spAuthProcessor) {
    std::ostringstream osExMsg;
    throw cta::exception::Exception("In grpc::AsyncServer::run(): Incorrect authorization processor.");
  }
  /*
   * Set auth prosess
   */
  spServerCredentials->SetAuthMetadataProcessor(spAuthProcessor);
  m_spAuthProcessor = spAuthProcessor;
  /*
   * Successful initilization
   * Let's build the server
   */
  std::string strAddress = "0.0.0.0:" + std::to_string(m_uiPort);
  lc.log(cta::log::INFO, "In grpc::AsyncServer::run(): Server is starting.");
  /*
   * Enlists an endpoint addr (port with an optional IP address) to bind the grpc::Server object to be created to.
   * It can be invoked multiple times.
   * strAddress: Valid values include dns:///localhost:1234, 192.168.1.1:31416, dns:///[::1]:27182, etc.
   * spServerCredentials: The credentials associated with the server. 
   */
  m_upServerBuilder->AddListeningPort(strAddress, spServerCredentials);
  m_upServer = m_upServerBuilder->BuildAndStart();
  // Initialise all registered handlers
  for (const auto& item : m_umapHandlers) {
    const std::unique_ptr<cta::frontend::grpc::request::IHandler>& upIHandler = item.second;
    upIHandler.get()->next(true);
    //TODO: Log names of initialised handlers;
  }
  // Proceed to the server's main loop.
  m_vThreads.resize(m_uiNoThreads);
  unsigned int uiThreadId = 0;
  for (std::thread& worker : m_vThreads) {
    std::thread t(&AsyncServer::process, this, uiThreadId);
    worker.swap(t);
    uiThreadId++;
  }
  {
    log::ScopedParamContainer params(lc);
    params.add("address", strAddress);
    params.add("threads", m_uiNoThreads);
    lc.log(cta::log::INFO, "In grpc::AsyncServer::run(): Server is listening.");
  }
  for (std::thread& worker : m_vThreads) {
    worker.join();
  }
}

void cta::frontend::grpc::server::AsyncServer::process(unsigned int uiId) {
  //TODO: break condition

  log::LogContext lc(m_log);
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
      params.add("thread", uiId);
      lc.log(cta::log::ERR, "In grpc::AsyncServer::process(): The completion queue has been shutdown.");
      break;
    }
    if (!pTag) {
      log::ScopedParamContainer params(lc);
      params.add("thread", uiId);
      lc.log(cta::log::ERR, "In grpc::AsyncServer::process(): Invalid tag delivered by notification queue.");
      break;
    }

    try {
      // Everything is ok the request can be processed
      cta::frontend::grpc::request::IHandler& handler =
        getHandler(static_cast<cta::frontend::grpc::request::Tag>(pTag));
      if (!handler.next(bOk)) {
        releaseHandler(static_cast<cta::frontend::grpc::request::Tag>(pTag));
      }
    }
    catch (const cta::exception::Exception& ex) {
      log::ScopedParamContainer params(lc);
      params.add("thread", uiId);
      params.add("message", ex.getMessageValue());
      lc.log(log::ERR, "grpc::AsyncServer::process(): Got an exception.");
    }
  }
}
