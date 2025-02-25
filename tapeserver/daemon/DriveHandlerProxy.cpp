/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "DriveHandlerProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"

namespace cta::tape::daemon {

DriveHandlerProxy::DriveHandlerProxy(server::SocketPair& socketPair, cta::log::LogContext& lc): m_socketPair(socketPair), m_lc(lc) {}

DriveHandlerProxy::~DriveHandlerProxy() {
  if (m_refreshLoggerClosingSock) {
    m_refreshLoggerClosing = true;
    // Send a signal to stop the waiting thread
    try {
      m_refreshLoggerClosingSock->send("stop_thread", server::SocketPair::Side::child);
      m_refreshLoggerAsyncFut.wait();
    } catch (std::exception &ex) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("exceptionMessage", ex.what());
      m_lc.log(log::ERR, "In DriveHandlerProxy::~DriveHandlerProxy(): received a std::exception.");
    } catch (...) {
      m_lc.log(log::ERR, "In DriveHandlerProxy::~DriveHandlerProxy(): received an unknown exception.");
    }
  }
}

void DriveHandlerProxy::addLogParams(const std::list<cta::log::Param> &params) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(false);
  for (auto & p: params) {
    auto * lp = watchdogMessage.mutable_addedlogparams()->Add();
    lp->set_name(p.getName());
    lp->set_value(p.getValueStr());
  }
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::addLogParams(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  try {
    m_socketPair.send(buffer, server::SocketPair::Side::parent);
  } catch (cta::exception::Exception& e) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("bufferLength", buffer.length());
      exParams.add("bufferMsg", buffer.substr(0, 4096));
      m_lc.log(log::ERR, "In DriveHandlerProxy::addLogParams(): Socket send failed.");
      throw e;
  }
}

void DriveHandlerProxy::deleteLogParams(const std::list<std::string> &paramNames) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(false);
  for (auto &pn: paramNames) {
    auto * lpn = watchdogMessage.mutable_deletedlogparams()->Add();
    *lpn = pn;
  }
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::deleteLogParams(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  try {
    m_socketPair.send(buffer, server::SocketPair::Side::parent);
  } catch (cta::exception::Exception& e) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("bufferLength", buffer.length());
      exParams.add("bufferMsg", buffer.substr(0, 4096));
      m_lc.log(log::ERR, "In DriveHandlerProxy::deleteLogParams(): Socket send failed.");
      throw e;
  }
}

void DriveHandlerProxy::resetLogParams() {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(false);
  watchdogMessage.set_resetlogparams(true);
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::resetLogParams(): could not serialize: ")+
                                    watchdogMessage.InitializationErrorString());
  }
  try {
    m_socketPair.send(buffer, server::SocketPair::Side::parent);
  } catch (cta::exception::Exception& e) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("bufferLength", buffer.length());
      exParams.add("bufferMsg", buffer.substr(0, 4096));
      m_lc.log(log::ERR, "In DriveHandlerProxy::resetLogParams(): Socket send failed.");
      throw e;
  }
}

void DriveHandlerProxy::labelError(const std::string& unitName, const std::string& message) {
  // TODO
  throw cta::exception::Exception("In DriveHandlerProxy::labelError(): not implemented");
}

void DriveHandlerProxy::setRefreshLoggerHandler(std::function<void()> handler) {
  // Setup async thread to handle incoming requests to refresh the logger
  if (!m_refreshLoggerHandler) {
    m_refreshLoggerHandler = handler;
    m_refreshLoggerClosingSock = std::make_unique<cta::server::SocketPair>();
    m_refreshLoggerAsyncFut = std::async(std::launch::async, [this] {
      // Create a new log context to ensure we don't have concurrent modifications on m_params
      cta::log::LogContext lc(m_lc.logger());
      try {
        server::SocketPair::pollMap pollList;
        pollList["0"] = &m_socketPair;
        pollList["1"] = m_refreshLoggerClosingSock.get();
        lc.log(log::INFO, "In DriveHandlerProxy::setRefreshLoggerHandler(): Waiting for refresh logger signal.");
        while (!m_refreshLoggerClosing) {
          try {
            server::SocketPair::poll(pollList, 300,
                                     server::SocketPair::Side::parent); // Add a 5 minute timeout, as a fallback in case we get stuck during shutdown
          } catch (server::SocketPair::Timeout &) {
            // Do nothing
            continue;
          }
          if (m_socketPair.pollFlag()) {
            m_socketPair.receive(server::SocketPair::Side::parent);
            auto handler = m_refreshLoggerHandler.value();
            handler();
          }
        }
      } catch(cta::exception::Exception & ex) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("exceptionMessage", ex.getMessageValue());
        lc.log(log::ERR, "In DriveHandlerProxy::setRefreshLoggerHandler(): received an exception. Backtrace follows.");
        lc.logBacktrace(log::INFO, ex.backtrace());
      } catch(std::exception &ex) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("exceptionMessage", ex.what());
        lc.log(log::ERR, "In DriveHandlerProxy::setRefreshLoggerHandler(): received a std::exception.");
      }
    });
  } else {
    throw cta::exception::Exception("In DriveHandlerProxy::setRefreshLoggerHandler(): refresh logger handler is already set");
  }
}

void DriveHandlerProxy::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(true);
  watchdogMessage.set_totaltapebytesmoved(totalTapeBytesMoved);
  watchdogMessage.set_totaldiskbytesmoved(totalDiskBytesMoved);
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportHeartbeat(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  try {
    m_socketPair.send(buffer, server::SocketPair::Side::parent);
  } catch (cta::exception::Exception& e) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("bufferLength", buffer.length());
      exParams.add("bufferMsg", buffer.substr(0, 4096));
      m_lc.log(log::ERR, "In DriveHandlerProxy::reportHeartbeat(): Socket send failed.");
      throw e;
  }
}

void DriveHandlerProxy::reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(true);
  watchdogMessage.set_reportingbytes(false);
  watchdogMessage.set_totaldiskbytesmoved(0);
  watchdogMessage.set_totaltapebytesmoved(0);
  watchdogMessage.set_sessionstate((uint32_t)state);
  watchdogMessage.set_sessiontype((uint32_t)type);
  watchdogMessage.set_vid(vid);
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportState(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  try {
    m_socketPair.send(buffer, server::SocketPair::Side::parent);
  } catch (cta::exception::Exception& e) {
      log::ScopedParamContainer exParams(m_lc);
      exParams.add("bufferLength", buffer.length());
      exParams.add("bufferMsg", buffer.substr(0, 4096));
      m_lc.log(log::ERR, "In DriveHandlerProxy::reportState(): Socket send failed.");
      throw e;
  }
}

} // namespace cta::tape::daemon
