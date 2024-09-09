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

#pragma once

#include "common/threading/SocketPair.hpp"
#include "common/log/LogContext.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"

#include <future>

namespace cta::tape::daemon {

/**
 * A class sending the data transfer process reports to the daemon via a
 * socketPair. It implements the TapedProxy interface.
 */
class DriveHandlerProxy: public TapedProxy {
public:
  /**
   * Constructor. The constructor will close the server side of the
   * pair.
   * @param sopcketPair Reference to the socket pair.
   */
  explicit DriveHandlerProxy(server::SocketPair & sopcketPair, cta::log::LogContext& lc);
  ~DriveHandlerProxy() override;
  void reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) override;
  void reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) override;
  void addLogParams(const std::list<cta::log::Param> &params) override;
  void deleteLogParams(const std::list<std::string> &paramNames) override;
  void resetLogParams() override;
  void labelError(const std::string& unitName, const std::string& message) override;
  void setRefreshLoggerHandler(std::function<void()> handler) override;
private:
  server::SocketPair & m_socketPair;

  // Handle broadcasting of refresh loger messages from parent to child process
  std::unique_ptr<server::SocketPair> m_refreshLoggerClosingSock;
  std::optional<std::function<void()>> m_refreshLoggerHandler;
  std::future<void> m_refreshLoggerAsyncFut;
  std::atomic<bool> m_refreshLoggerClosing = false;
  // The log context
  cta::log::LogContext& m_lc;
};

} // namespace cta::tape::daemon
